package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskStatus int

const (
	idle TaskStatus = iota
	inProgress
	completed
)

type TaskType int

const (
	MAP TaskType = iota
	REDUCE
)

type MapTask struct {
	Id        int
	Filename  string
	ReduceNum int

	status TaskStatus
}
type ReduceTask struct {
	Id int

	status TaskStatus
}

type ReplyStatus int

const (
	HasTask ReplyStatus = iota
	Waiting
	NoMoreTask
)

type AskForTaskArgs struct {
}

type AskForTaskReply struct {
	Status     ReplyStatus
	TaskType   TaskType
	MapTask    MapTask
	ReduceTask ReduceTask
}

type FinishTaskArgs struct {
	Id       int
	TaskType TaskType
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
