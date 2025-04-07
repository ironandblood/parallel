package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func emitIntermideate(dest string, res []KeyValue) {
	// the last parm is just the right in Linux
	file, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0660)
	defer file.Close()
	enc := json.NewEncoder(file)
	if err != nil {
		log.Fatalf("Can not write data to %v\n", dest)
	}
	defer file.Close()
	for _, kv := range res {
		err = enc.Encode(&kv)
	}
	if err != nil {
		log.Fatalf("Json read failed\n")
	}

}

func decode(file *os.File) []KeyValue {
	dec := json.NewDecoder(file)
	var kva []KeyValue
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	done := false
	for !done {
		// ask for master's instruction
		args := TaskReq{-1, -1}
		reply := TaskAns{}
		ok := call("Coordinator.Schedule", &args, &reply)
		if !ok {
			log.Fatalln("Worker Call failed")
			continue
		}
		// fmt.Printf("we send %v and receive %v\n", args, reply)
		switch reply.TaskType {
		case 0: // map task
			content, err := os.ReadFile(reply.FileName)
			if err != nil {
				log.Printf("Req %v,\n Ans %v", args, reply)
				log.Fatalf("Worker read file %v failed %v\n", reply.FileName, err)
			}
			// call map fun
			res := mapf(reply.FileName, string(content))
			// write to intermidiate file
			buckets := make(map[int][]KeyValue, reply.NReduce)
			for _, kv := range res {
				dest_bucket := ihash(kv.Key) % reply.NReduce
				buckets[dest_bucket] = append(buckets[dest_bucket], kv)
			}

			for k, v := range buckets {
				dest_file := fmt.Sprintf("mr-%v-%v", reply.MapTaskNumber, k)
				emitIntermideate(dir_prefix+dest_file, v)
			}

			// notify master
			task_done := TaskReq{reply.MapTaskNumber, -1}
			task_reply := TaskAns{}
			ok := call("Coordinator.TaskDone", &task_done, &task_reply)
			if !ok {
				log.Fatalln("Ack task failed")
			}
		case 1: // reduce task
			// get kva to reduce
			// fmt.Printf("Reduce task %v\n", reply)
			files, _ := os.ReadDir(dir_prefix)
			re := regexp.MustCompile("mr-[0-9]*-" + strconv.Itoa(reply.ReduceTaskNumber))
			var kva []KeyValue
			for _, f := range files {
				cond := re.MatchString(f.Name())
				if cond {
					file, err := os.Open(dir_prefix + f.Name())
					if err != nil {
						log.Fatalf("cannot open %v", f)
					}
					kva = append(kva, decode(file)...)
					file.Close()
				}
			}
			// sort kvas
			sort.Sort(ByKey(kva))

			// run reduce func and write to target
			i := 0
			ofile, err := os.Create("mr-out-" + strconv.Itoa(reply.ReduceTaskNumber))
			if err != nil {
				log.Fatalf("cannot create output file")
			}
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			ofile.Close()
			// tell master reduce task done
			task_done := TaskReq{-1, reply.ReduceTaskNumber}
			task_reply := TaskAns{}
			ok := call("Coordinator.TaskDone", &task_done, &task_reply)
			if !ok {
				log.Fatalln("Ack task failed")
			}
			// fmt.Printf("REDUCE DONE %v\n", reply)
		case 2: // task done, terminate
			done = true
		case 3:
			time.Sleep(1)
			continue
		default: // something wrong in master
			log.Fatalln("Master wrong")
		}

	}
}

/*
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	done := false
	for !done {
		args := AskForTaskReq{}
		reply := AskForTaskAns{}

		ok := call("Coordinator.Schedule", &args, &reply)
		if !ok {
			fmt.Println("Call failed")
		}

		// At least, we got instruction
		switch t := reply.task.(type) {
		case MapTask:

			res := mapf(t.key, t.value)
			fileName := fmt.Sprintf("mr-inter-%v", t.key)
			emitIntermideate(fileName, res)
		case ReduceTask:
			reducef(t.key, t.value)
		case Terminate:
			done = true
		}

		res_args := TaskDoneMsg{reply.task}
		res_reply := TaskDoneRes{}
		ok = call("Coordinator.Finish", &res_args, &res_reply)
		if !ok {
			fmt.Println("Call failed")
		}
	}

}
*/

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
