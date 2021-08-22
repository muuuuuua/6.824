package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"


type ReplyStatus int

const (
	HasTask ReplyStatus = iota
	NoMoreTask
)

type AskForTaskArgs struct {
}

type AskForTaskReply struct {
	Status ReplyStatus
	Task   MapTask
}

type FinishTaskArgs struct {
	Id int
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
