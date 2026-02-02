package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
	workerId := os.Getpid()
	for {
		log.Println("Requesting task")
		time.Sleep(time.Second)

		getTaskArgs := &GetTaskArgs{WorkerId: workerId}
		getTaskReply := &GetTaskReply{}
		if !call("Coordinator.GetTask", &getTaskArgs, &getTaskReply) {
			log.Fatal("Error when calling Coordinator RPC server")
		}

		switch getTaskReply.Action {
		case Wait:
			time.Sleep(time.Second * 2)
		case Exit:
			log.Printf("All tasks have been completed. Stopping worker %v\n", workerId)
			return
		case Run:
			if getTaskReply.TaskType == Map {
				handleMapTask(workerId, getTaskReply.MapFile, getTaskReply.TaskID, getTaskReply.NReduce, mapf)
			}
		}
	}
}

func handleMapTask(workerId int, filename string, taskID int, nReduce int, mapf func(string, string) []KeyValue) {
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

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

	log.Println("Reporting task")
	args := &ReportTaskArgs{WorkerId: workerId, TaskType: Map, TaskID: taskID}
	reply := &ReportTaskReply{}
	if !call("Coordinator.ReportTask", &args, &reply) {
		log.Fatal("Error when calling Coordinator RPC server")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		fmt.Printf("reply.Y %v\n", reply.Y)
// 	} else {
// 		fmt.Printf("call failed!\n")
// 	}
// }

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
