package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMap(task *MapTask, mapf func(string, string) []KeyValue) {
	reduceNum := task.ReduceNum
	taskId := task.Id
	filename := task.Filename
	log.Printf("begin to do map task[%d] with %s", taskId, filename)

	intermediateFile := []*os.File{}
	for i := 0; i < reduceNum; i++ {
		filename := fmt.Sprintf("mr-%d-%d", taskId, i)
		a, _ := os.Create(filename)
		intermediateFile = append(intermediateFile, a)
	}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	_ = file.Close()
	kva := mapf(filename, string(content))
	for _, kv := range kva {
		index := ihash(kv.Key) % reduceNum
		_, _ = fmt.Fprintf(intermediateFile[index], "%v %v\n", kv.Key, kv.Value)
	}

	for _, f := range intermediateFile {
		_ = f.Close()
	}

	FinishTask(taskId, MAP)
}

func doReduce(task *ReduceTask, reducef func(string, []string) string) {
	taskId := task.Id
	log.Printf("begin to do reduce task[%d]", taskId)
	time.Sleep(1 * time.Second)
	FinishTask(taskId, REDUCE)
}

// Worker
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	stop := false
	for !stop {
		reply := AskForTaskReply{}
		AskForTask(&reply)
		if reply.Status == HasTask {
			if reply.TaskType == MAP {
				t := reply.MapTask
				doMap(&t, mapf)
			}
			if reply.TaskType == REDUCE {
				t := reply.ReduceTask
				doReduce(&t, reducef)
			}
		} else if reply.Status == Waiting {
			time.Sleep(1 * time.Second)
		} else {
			log.Println("no more tasks, worker exit")
			stop = true
		}
	}
}

func AskForTask(reply *AskForTaskReply) {
	args := AskForTaskArgs{}

	call("Coordinator.AskForTask", &args, &reply)
}

func FinishTask(id int, taskType TaskType) {
	log.Printf("finish task %d", id)
	args := FinishTaskArgs{
		Id:       id,
		TaskType: taskType,
	}
	reply := FinishTaskReply{}

	call("Coordinator.FinishTask", &args, &reply)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
