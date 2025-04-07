package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskReq struct {
	MapKey    int
	ReduceKey int
}

type TaskAns struct {
	TaskType int // 0:map, 1:reduce, 2:terminate, 3:nop, else:error
	NMap     int // n map
	NReduce  int // n reduce

	MapTaskNumber int    // pos in map
	FileName      string // file to map

	ReduceTaskNumber int // pos in reduce
}

const dir_prefix string = "/Users/bytedance/dev/6.5840/src/tmp/"

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
