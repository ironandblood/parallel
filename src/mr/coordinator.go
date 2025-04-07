package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type MapBlock struct {
	id     int
	file   string
	done   bool
	runing bool
	time   time.Time
}

type ReduceBlock struct {
	id     int
	done   bool
	runing bool
	time   time.Time
}

type Coordinator struct {
	// Your definitions here.
	nReduce    int
	nMap       int
	mapTask    []MapBlock
	reduceTask []ReduceBlock
	mapDone    bool
	reduceDone bool

	mapToRun    int
	reduceToRun int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Demo(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X * 10
	return nil
}

func (c *Coordinator) DisplayStatues() {
	fmt.Println("===================Map progress")
	for i, v := range c.mapTask {
		star := []byte("  ")
		if v.runing {
			star[0] = '*'
		}
		if v.done {
			star[1] = '.'
		}
		s := string(star)
		fmt.Printf("%v %v\t", i, s)
	}
	fmt.Println()
	fmt.Println("====================Reduce progress")
	for i, v := range c.reduceTask {
		star := []byte("  ")
		if v.runing {
			star[0] = '*'
		}
		if v.done {
			star[1] = '.'
		}
		s := string(star)
		fmt.Printf("%v %v\t", i, s)
	}
	fmt.Println()
}

func (c *Coordinator) Schedule(args *TaskReq, reply *TaskAns) error {
	// fmt.Printf("*")
	if args.MapKey == -1 && args.ReduceKey == -1 {
		reply.NMap = c.nMap
		reply.NReduce = c.nReduce

		if !c.mapDone { // assign a map task
			for i, t := range c.mapTask {
				// two cases, assigned but time out or not assigned
				if (t.runing && time.Now().After(c.mapTask[i].time.Add(time.Second*10))) || (!t.done && !t.runing) {
					reply.TaskType = 0
					reply.FileName = t.file
					reply.MapTaskNumber = i

					c.mapTask[i].time = time.Now()
					c.mapTask[i].runing = true
					return nil
				}
			}
			reply.TaskType = 3
			return nil
		} else if !c.reduceDone { // assign a reduce task
			for i, t := range c.reduceTask {
				if (t.runing && time.Now().After(c.reduceTask[i].time.Add(time.Second*10))) || (!t.done && !t.runing) {
					// unfinished and overtime
					reply.TaskType = 1
					reply.ReduceTaskNumber = i

					c.reduceTask[i].time = time.Now()
					c.reduceTask[i].runing = true
					return nil
				}
			}
			reply.TaskType = 3
			return nil
		} else { // terminate
			reply.TaskType = 2
			return nil
		}
	} else {
		log.Fatalln("Unknown request msg")

	}
	return nil

}

func (c *Coordinator) TaskDone(args *TaskReq, reply *TaskAns) error {
	// fmt.Printf("Task %v\n", args)
	// c.DisplayStatues()
	if args.MapKey != -1 && args.ReduceKey == -1 {
		if !c.mapTask[args.MapKey].done {
			c.mapToRun -= 1
		}
		c.mapTask[args.MapKey].done = true
		c.mapTask[args.MapKey].runing = false
		if c.mapToRun == 0 {
			c.mapDone = true
			// fmt.Println("Map Done")
		}
	} else if args.MapKey == -1 && args.ReduceKey != -1 {
		if !c.reduceTask[args.ReduceKey].done {
			c.reduceToRun -= 1
		}
		c.reduceTask[args.ReduceKey].done = true
		c.reduceTask[args.ReduceKey].runing = false
		if c.reduceToRun == 0 {
			c.reduceDone = true
			// fmt.Println("DONE")
			time.Sleep(1)
		}
	}
	// c.DisplayStatues()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)  // Register functions
	rpc.HandleHTTP() // Use http protocal
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := c.mapDone && c.reduceDone
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce, c.nMap = nReduce, len(files)
	c.mapDone, c.reduceDone = false, false
	c.mapTask, c.reduceTask = make([]MapBlock, c.nMap), make([]ReduceBlock, nReduce)
	c.mapToRun, c.reduceToRun = c.nMap, c.nReduce

	for i := 0; i < c.nMap; i += 1 {
		c.mapTask[i].id = i
		c.mapTask[i].file = files[i]
		c.mapTask[i].done = false
	}

	for i := 0; i < c.nReduce; i += 1 {
		c.reduceTask[i].id = i
		c.reduceTask[i].done = false
	}

	os.RemoveAll(dir_prefix)
	os.Mkdir(dir_prefix, os.ModePerm)
	c.server()
	return &c
}
