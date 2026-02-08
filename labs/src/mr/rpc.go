package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
type GetTaskArgs struct {
	WorkerId int
}

type GetTaskReply struct {
	TaskType TaskType   // Map, Reduce
	TaskID   int        // Unique task ID
	NMap     int        // Number of Map Tasks. Needed for reduce step
	NReduce  int        // Number of Reduce tasks. Needed to create NReduce files
	MapFile  string     // File to process using Map func
	Action   TaskAction // Wait, Exit, Run
}

type ReportTaskArgs struct {
	WorkerID int
	TaskType TaskType
	TaskID   int
}

type ReportTaskReply struct {
	Ok bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
