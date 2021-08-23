package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

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
			reply.TaskType = MAP
			reply.MapTask = f
			return nil
		}
	}
	// no map task found
	// todo do not traverse list
	for _, t := range c.mapTaskList {
		if t.status != completed {
			// at least one map task not finished, worker need to wait
			reply.Status = Waiting
			return nil
		}
	}

	for _, f := range c.reduceTaskList {
		if f.status == idle {
			f.status = inProgress
			reply.Status = HasTask
			reply.TaskType = REDUCE
			reply.ReduceTask = f
			return nil
		}
	}
	reply.Status = NoMoreTask
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.m.Lock()
	defer c.m.Unlock()
	if args.TaskType == MAP {
		c.mapTaskList[args.Id].status = completed
	} else {
		c.reduceTaskList[args.Id].status = completed
	}
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

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// todo do not traverse list
	c.m.Lock()
	defer c.m.Unlock()
	for _, f := range c.reduceTaskList {
		if f.status != completed {
			return false
		}
	}

	return true
}

// MakeCoordinator
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
		c.mapTaskList = append(c.mapTaskList, MapTask{Id: i, Filename: f, ReduceNum: nReduce, status: idle})
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTaskList = append(c.reduceTaskList, ReduceTask{Id: i, status: idle})
	}

	c.server()
	return &c
}
