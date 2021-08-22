package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStatus int

const (
	idle TaskStatus = iota
	inProgress
	completed
)

type MapTask struct {
	Id        int
	Filename  string
	ReduceNum int

	status TaskStatus
}
type ReduceTask struct {
	Id int
}

type Coordinator struct {
	files          []string
	mapTaskList    []MapTask
	reduceTaskList []ReduceTask
	m              sync.Mutex
}

func (c *Coordinator) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	c.m.Lock()
	defer c.m.Unlock()
	for _, f := range c.mapTaskList {
		if f.status == idle {
			f.status = inProgress
			reply.Status = HasTask
			reply.Task = f
			return nil
		}
	}
	// no map task found
	// todo
	// 1. waiting map tasks finish
	// 2. begin to assign reduce tasks
	reply.Status = NoMoreTask
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.m.Lock()
	defer c.m.Unlock()
	c.mapTaskList[args.Id].status = completed

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.Println("coordinator init with files: ", files)
	c := Coordinator{
		files: files,
	}
	for i, f := range c.files {
		c.mapTaskList = append(c.mapTaskList, MapTask{Id: i, Filename: f, ReduceNum: nReduce})
	}
	// todo init reduce tasks

	c.server()
	return &c
}
