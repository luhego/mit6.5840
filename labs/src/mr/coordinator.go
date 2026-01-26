package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type TaskType int

const (
	Map TaskType = iota
	Reduce
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Done
)

type Task struct {
	TaskID    int
	Type      TaskType  // Map, Reduce
	State     TaskState // Idle, InProgress, Done
	WorkerID  int
	StartTime time.Time
	Input     string
}

type Coordinator struct {
	// Your definitions here.
	NMap        int
	NReduce     int
	MapTasks    []Task
	ReduceTasks []Task
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	// complete it
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	for _, task := range c.ReduceTasks {
		if task.State != Done {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Create map tasks
	mapTasks := createMapTasks(files)
	// Create reduce tasks
	reduceTasks := createReduceTasks(nReduce)

	c := Coordinator{NMap: len(files), NReduce: nReduce, MapTasks: mapTasks, ReduceTasks: reduceTasks}

	c.server()
	return &c
}

func createMapTasks(files []string) []Task {
	tasks := make([]Task, len(files))
	for i, file := range files {
		tasks[i] = Task{Type: Map, State: Idle, Input: file}
	}
	return tasks
}

func createReduceTasks(nReduce int) []Task {
	tasks := make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		tasks[i] = Task{Type: Reduce, State: Idle}
	}
	return tasks
}
