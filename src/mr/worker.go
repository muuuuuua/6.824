package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	var intermediateFile []*os.File
	var intermediateFilename []string
	for i := 0; i < reduceNum; i++ {
		f := fmt.Sprintf("mr-%d-%d", taskId, i)
		a, _ := os.Create(f)
		intermediateFile = append(intermediateFile, a)
		intermediateFilename = append(intermediateFilename, f)
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

	var jsonFile []*json.Encoder
	for _, f := range intermediateFile {
		jsonFile = append(jsonFile, json.NewEncoder(f))
	}
	for _, kv := range kva {
		index := ihash(kv.Key) % reduceNum
		_ = jsonFile[index].Encode(&kv)
	}

	for _, f := range intermediateFile {
		_ = f.Close()
	}

	FinishMapTask(taskId, intermediateFilename)
}

func doReduce(task *ReduceTask, reducef func(string, []string) string) {
	taskId := task.Id
	fileList := task.InputFileList
	log.Printf("begin to do reduce task[%d] with %v", taskId, fileList)

	var intermediate []KeyValue
	for _, filename := range fileList {
		f, err := os.Open(filename)
		if err != nil {
			log.Printf("open file %s failed", filename)
			continue
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		_ = f.Close()
	}

	sort.Sort(ByKey(intermediate))

	outputName := fmt.Sprintf("mr-out-%d", taskId)
	outputFile, _ := os.Create(outputName)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, _ = fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	_ =outputFile.Close()
	FinishReduceTask(taskId)
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

func FinishMapTask(id int, reduceFileList []string) {
	log.Printf("finish map task %d", id)
	args := FinishMapTaskArgs{
		Id:             id,
		ReduceFileList: reduceFileList,
	}
	reply := FinishMapTaskReply{}

	call("Coordinator.FinishMapTask", &args, &reply)
}

func FinishReduceTask(id int) {
	log.Printf("finish reduce task %d", id)
	args := FinishReduceTaskArgs{
		Id: id,
	}
	reply := FinishReduceTaskReply{}

	call("Coordinator.FinishReduceTask", &args, &reply)
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
