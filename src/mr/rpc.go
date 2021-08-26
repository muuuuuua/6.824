package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
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

	startTime time.Time
	status    TaskStatus
}

type ReduceTask struct {
	Id            int
	InputFileList []string

	startTime time.Time
	status    TaskStatus
}

func (t *MapTask) Idle() bool {
	if t.status == idle {
		return true
	}
	if t.status == inProgress && time.Now().Sub(t.startTime) >= 10*time.Second {
		return true
	}
	return false
}

func (t *ReduceTask) Idle() bool {
	if t.status == idle {
		return true
	}
	if t.status == inProgress && time.Now().Sub(t.startTime) >= 10*time.Second {
		return true
	}
	return false
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

type FinishMapTaskArgs struct {
	Id             int
	ReduceFileList []string
}

type FinishMapTaskReply struct {
}

type FinishReduceTaskArgs struct {
	Id int
}

type FinishReduceTaskReply struct {
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
