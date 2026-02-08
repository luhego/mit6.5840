package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// For sorting during reduce
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const pollInterval = 1 * time.Second
const waitInterval = 2 * time.Second

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	workerID := os.Getpid()
	for {
		time.Sleep(pollInterval)

		log.Println("Requesting task")
		args := &GetTaskArgs{WorkerId: workerID}
		reply := &GetTaskReply{}
		if !call("Coordinator.GetTask", &args, &reply) {
			log.Fatal("Error when calling Coordinator RPC server")
		}

		switch reply.Action {
		case Wait:
			time.Sleep(waitInterval)
		case Exit:
			log.Printf("All tasks have been completed. Stopping worker %v\n", workerID)
			return
		case Run:
			if reply.TaskType == Map {
				handleMapTask(workerID, reply.MapFile, reply.TaskID, reply.NReduce, mapf)
			} else {
				handleReduceTask(workerID, reply.TaskID, reply.NMap, reducef)
			}
		}
	}
}

func handleMapTask(workerID int, filename string, taskID int, nReduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	intermediate := mapf(filename, string(content))
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range intermediate {
		bIndex := ihash(kv.Key) % nReduce
		buckets[bIndex] = append(buckets[bIndex], kv)
	}

	for r, bucket := range buckets {
		oname := fmt.Sprintf("mr-%d-%d", taskID, r)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range bucket {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v", err)
			}
		}
		ofile.Close()
	}

	reportTask(workerID, Map, taskID)
}

func handleReduceTask(workerID int, taskID int, nMap int, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for m := range nMap {
		filename := fmt.Sprintf("mr-%d-%d", m, taskID)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", taskID)
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	reportTask(workerID, Reduce, taskID)
}

func reportTask(workerID int, taskType TaskType, taskID int) {
	log.Println("Reporting task")
	args := &ReportTaskArgs{WorkerID: workerID, TaskType: taskType, TaskID: taskID}
	reply := &ReportTaskReply{}
	if !call("Coordinator.ReportTask", &args, &reply) {
		log.Fatal("Error when calling Coordinator RPC server")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
