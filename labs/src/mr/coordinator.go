package mr

import (
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskType int

const (
	Map TaskType = iota
	Reduce
)

type TaskState int

const (
	ToBeStarted TaskState = iota
	InProgress
	Done
)

type TaskAction int

const (
	Wait TaskAction = iota
	Exit
	Run
)

type Task struct {
	TaskID    int
	Type      TaskType  // Map, Reduce
	State     TaskState // ToBeStarted, InProgress, Done
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
	mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	// Check if any Map task is in state ToBeStarted
	doneCounter := 0
	for _, task := range c.MapTasks {
		switch task.State {
		case ToBeStarted:
			reply.TaskType = task.Type
			reply.TaskID = task.TaskID
			reply.NMap = c.NMap
			reply.NReduce = c.NReduce
			reply.MapFile = task.Input
			reply.Action = Run
			return nil
		case Done:
			doneCounter++
		}
	}

	// Map phase hasn't completed yet
	if doneCounter < c.NMap {
		return nil
	}

	// Check if any Reduce task is in state ToBeStarted
	for _, task := range c.ReduceTasks {
		if task.State == ToBeStarted {
			reply.TaskType = task.Type
			reply.TaskID = task.TaskID
			reply.NMap = c.NMap
			reply.NReduce = c.NReduce
			reply.Action = Run
		}
	}

	return nil
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
	jsonHandler := slog.NewJSONHandler(os.Stderr, nil)
	slog := slog.New(jsonHandler)

	// Create map tasks
	mapTasks := createMapTasks(files)
	// Create reduce tasks
	reduceTasks := createReduceTasks(nReduce)

	c := Coordinator{NMap: len(files), NReduce: nReduce, MapTasks: mapTasks, ReduceTasks: reduceTasks}

	slog.Info("Coordinator created", "NMap", c.NMap, "NReduce", c.NReduce)

	c.server()
	return &c
}

func createMapTasks(files []string) []Task {
	nextTaskId := 0
	tasks := make([]Task, len(files))
	for i, file := range files {
		tasks[i] = Task{TaskID: nextTaskId, Type: Map, State: ToBeStarted, Input: file}
		nextTaskId++
	}
	return tasks
}

func createReduceTasks(nReduce int) []Task {
	nextTaskId := 0
	tasks := make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		tasks[i] = Task{TaskID: nextTaskId, Type: Reduce, State: ToBeStarted}
		nextTaskId++
	}
	return tasks
}
