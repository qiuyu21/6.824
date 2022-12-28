package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

type KeyValue struct {
	Key   string
	Value string
}

type SortedKey []KeyValue
func (a SortedKey) Len() int           { return len(a) }
func (a SortedKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortedKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		var (
			req_task_args RequestTaskArgs
			req_task_repl RequestTaskReply
			fin_task_args FinishTaskArgs
			fin_task_repl FinishTaskReply
		)
		if !call("Coordinator.RPCRequestTask", &req_task_args, &req_task_repl) {
			break
		}
		switch req_task_repl.TaskType {
		case TaskTypeNil:
			return
		case TaskTypeMap:
			handleMapTask(&req_task_repl, mapf)
		case TaskTypeRed:
			handleRedTask(&req_task_repl, reducef)
		}
		fin_task_args.TaskId = req_task_repl.TaskId
		fin_task_args.AssignedTime = req_task_repl.AssignedTime
		if !call("Coordinator.RPCFinishTask", &fin_task_args, &fin_task_repl) {
			break
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

func handleMapTask(req *RequestTaskReply, mapf func(string, string) []KeyValue) {
	content := readFile(req.Filename)
	res := mapf(req.Filename, content)
	nReduce := req.NReduce

	var intermediate_files []*os.File
	var encoders []*json.Encoder

	for i := 0; i < nReduce; i++ {
		f, _ := os.Create(GetIntermediateFilename(req.TaskId, i))
		intermediate_files = append(intermediate_files, f)
		encoders = append(encoders, json.NewEncoder(f))
		defer f.Close()
	}

	for _, kv := range res {
		i := ihash(kv.Key) % nReduce
		encoders[i].Encode(&kv)
	}
}

func handleRedTask(req *RequestTaskReply, reducef func(string, []string) string) {
	var intermediate_files []*os.File
	var decoders []*json.Decoder

	for i := 0; i < req.NMap; i++ {
		f, _ := os.Open(GetIntermediateFilename(i, req.TaskId))
		intermediate_files = append(intermediate_files, f)
		decoders = append(decoders, json.NewDecoder(f))
		defer f.Close()
	}

	var kvs []KeyValue
	for _, decoder := range decoders {
		for decoder.More() {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				log.Fatalln("Decode failed")
			}
			kvs = append(kvs, kv)
		}
	}

	sort.Sort(SortedKey(kvs))

	f, _ := ioutil.TempFile("./", "MR")

	i := 0
	for i < len(kvs) {
		j := i + 1
		vals := []string{kvs[i].Value}
		for ;j < len(kvs) && kvs[i].Key == kvs[j].Key; j++ {
			vals = append(vals, kvs[j].Value)
		}
		output := reducef(kvs[i].Key, vals)
		fmt.Fprintf(f, "%v %v\n", kvs[i].Key, output)
		i = j
	}

	os.Rename(f.Name(), fmt.Sprintf("./mr-out-%v", req.TaskId))
}

func readFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}

// X: map task number
// Y: reduce task number
func GetIntermediateFilename(x int, y int) string {
	return fmt.Sprintf("mr-%v-%v", x, y)
}