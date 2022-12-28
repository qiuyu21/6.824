package mr

import (
	"os"
	"strconv"
)

type TaskType uint8

type TaskStatus uint8

const (
	TaskTypeNil TaskType = iota
	TaskTypeMap
	TaskTypeRed
)

const (
	TaskStatusCreated TaskStatus = iota
	TaskStatusPending
	TaskStatusFinished
)

type RequestTaskArgs struct {

}

type RequestTaskReply struct {
	TaskId int
	Filename string
	TaskType TaskType
	AssignedTime string
	NMap, NReduce int
}

type FinishTaskArgs struct {
	TaskId int
	AssignedTime string
}

type FinishTaskReply struct {

}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
