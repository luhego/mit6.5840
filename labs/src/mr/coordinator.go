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

const TaskTimeout = 10 * time.Second

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
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("GetTask. WorkerID: %d", args.WorkerId)

	// Map phase
	if idx, ok := findIdleTask(c.MapTasks); ok {
		c.assignTask(&c.MapTasks[idx], args.WorkerId)
		c.fillReplyForTask(reply, &c.MapTasks[idx])
		return nil
	}

	// Let worker wait if not all map tasks are done
	if !allDone(c.MapTasks) {
		reply.Action = Wait
		return nil
	}

	// Reduce phase
	if idx, ok := findIdleTask(c.ReduceTasks); ok {
		c.assignTask(&c.ReduceTasks[idx], args.WorkerId)
		c.fillReplyForTask(reply, &c.ReduceTasks[idx])
		return nil
	}

	// Let worker wait if not all reduce tasks are done
	if !allDone(c.ReduceTasks) {
		reply.Action = Wait
		return nil
	}

	// All tasks have been completed
	reply.Action = Exit
	return nil

}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	log.Printf("ReportTask. WorkerID: %d. TaskType: %d, TaskID: %d", args.WorkerID, args.TaskType, args.TaskID)
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == Map {
		// Stale worker
		if c.MapTasks[args.TaskID].WorkerID != args.WorkerID {
			return nil
		}

		c.MapTasks[args.TaskID].State = Done
	} else {
		// Stale worker
		if c.ReduceTasks[args.TaskID].WorkerID != args.WorkerID {
			return nil
		}
		c.ReduceTasks[args.TaskID].State = Done
	}

	reply.Ok = true
	return nil
}

// -------- HELPERS --------------
func (c *Coordinator) fillReplyForTask(reply *GetTaskReply, task *Task) {
	reply.TaskType = task.Type
	reply.TaskID = task.TaskID
	reply.NMap = c.NMap
	reply.NReduce = c.NReduce
	reply.MapFile = task.Input
	reply.Action = Run
}

func (c *Coordinator) assignTask(task *Task, workerID int) {
	task.State = InProgress
	task.StartTime = time.Now()
	task.WorkerID = workerID
}

func findIdleTask(tasks []Task) (int, bool) {
	for idx, task := range tasks {
		if task.State == ToBeStarted {
			return idx, true
		} else if task.State == InProgress && time.Since(task.StartTime) > TaskTimeout {
			return idx, true
		}
	}
	return -1, false
}

func allDone(tasks []Task) bool {
	for _, task := range tasks {
		if task.State != Done {
			return false
		}
	}
	return true
}

// -------- HELPERS --------------

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
	c.mu.Lock()
	defer c.mu.Unlock()
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
	tasks := make([]Task, len(files))
	for i, file := range files {
		tasks[i] = Task{TaskID: i, Type: Map, State: ToBeStarted, Input: file}
	}
	return tasks
}

func createReduceTasks(nReduce int) []Task {
	tasks := make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		tasks[i] = Task{TaskID: i, Type: Reduce, State: ToBeStarted}
	}
	return tasks
}
