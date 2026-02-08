package mr

import (
	"testing"
	"time"
)

func newTestCoord(nMap, nReduce int) *Coordinator {
	c := &Coordinator{
		NMap:        nMap,
		NReduce:     nReduce,
		MapTasks:    make([]Task, nMap),
		ReduceTasks: make([]Task, nReduce),
	}

	for i := 0; i < nMap; i++ {
		c.MapTasks[i] = Task{TaskID: i, Type: Map, State: ToBeStarted, Input: "f"}
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = Task{TaskID: i, Type: Reduce, State: ToBeStarted, Input: "f"}
	}

	return c
}

func TestGetTask_UpdatesState(t *testing.T) {
	c := newTestCoord(1, 1)
	args := &GetTaskArgs{WorkerId: 1}
	reply := &GetTaskReply{}

	_ = c.GetTask(args, reply)

	if c.MapTasks[0].State != InProgress {
		t.Fatalf("expected InProgress, got %v", c.MapTasks[0].State)
	}
}

func TestGetTask_GetSecondTask(t *testing.T) {
	c := newTestCoord(2, 1)
	c.MapTasks[0].State = Done
	args := &GetTaskArgs{WorkerId: 1}
	reply := &GetTaskReply{}

	_ = c.GetTask(args, reply)
	if reply.TaskID != 1 {
		t.Fatalf("expected Task 1, got %v", reply.TaskID)
	}

}

func TestGetTask_WaitBeforeAllMapsDone(t *testing.T) {
	c := newTestCoord(2, 1)
	c.MapTasks[0].State = Done
	c.MapTasks[1].State = InProgress
	c.MapTasks[1].StartTime = time.Now()

	args := &GetTaskArgs{WorkerId: 1}
	reply := &GetTaskReply{}
	_ = c.GetTask(args, reply)

	if reply.Action != Wait {
		t.Fatalf("expected Wait, got %v", reply.Action)
	}
}

func TestGetTask_AssignsReduceAfterMapsDone(t *testing.T) {
	c := newTestCoord(1, 1)
	c.MapTasks[0].State = Done

	args := &GetTaskArgs{WorkerId: 1}
	reply := &GetTaskReply{}
	_ = c.GetTask(args, reply)
	if reply.TaskType == Map {
		t.Fatalf("expected Reduce Run, go type%v action=%v", reply.TaskType, reply.Action)
	}
}

func TestGetTask_ExitWhenAllDone(t *testing.T) {
	c := newTestCoord(1, 1)
	c.MapTasks[0].State = Done
	c.ReduceTasks[0].State = Done

	args := &GetTaskArgs{WorkerId: 1}
	reply := &GetTaskReply{}
	_ = c.GetTask(args, reply)
	if reply.Action != Exit {
		t.Fatalf("expected Exit, got %v", reply.Action)
	}
}

func TestGetTask_ReassignsTimedOutMap(t *testing.T) {
	c := newTestCoord(1, 1)
	c.MapTasks[0].State = InProgress
	c.MapTasks[0].StartTime = time.Now().Add(-TaskTimeout - time.Second)

	args := &GetTaskArgs{WorkerId: 7}
	reply := &GetTaskReply{}
	_ = c.GetTask(args, reply)
	if reply.Action != Run || reply.TaskType != Map || reply.TaskID != 0 {
		t.Fatalf("expected reassigned Map task, to type=%v action%v id=%v", reply.TaskType, reply.Action, reply.TaskID)
	}
}

func TestReportTask_MarksDone(t *testing.T) {
	c := newTestCoord(1, 1)
	args := &ReportTaskArgs{WorkerId: 1, TaskType: 1, TaskID: 0}
	reply := &ReportTaskReply{}
	_ = c.ReportTask(args, reply)
	if c.MapTasks[0].State != Done {

	}
}

func TestReportTask_IgnoresStaleWorker(t *testing.T) {
	c := newTestCoord(1, 1)
	c.MapTasks[0].State = InProgress
	c.MapTasks[0].WorkerID = 1
	c.MapTasks[0].StartTime = time.Unix(0, 0)

	args := &ReportTaskArgs{WorkerId: 2, TaskType: Map, TaskID: 0}
	reply := &ReportTaskReply{}
	_ = c.ReportTask(args, reply)

	if c.MapTasks[0].State == Done {
		t.Fatalf("expected state report to be ignored")
	}
}
