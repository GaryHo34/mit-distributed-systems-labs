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
type WorkStatus int

const (
	NOTSTART WorkStatus = iota
	STARTED
	FINISHED
)

// the type of the work, MAP or REDUCE
type WorkType int

const (
	MAP WorkType = iota
	REDUCE
)

// The information needed for Map type work
type MapWork struct {
	Filename  string
	FileIndex int // This is a convention for mr-X index
	NReduce   int // how many reduce files
}

type ReduceWork struct {
	ReduceIndex int // to know witch mr-X-Y file it needs to work on
	NMapWork    int // how many map files
}

type Work struct {
	WorkType   WorkType
	MapWork    MapWork
	ReduceWork ReduceWork
}

type WorkReply struct {
	HasWork bool
	Work    Work
}

type WorkArgs struct {
	WorkType   WorkType
	MapWork    MapWork
	ReduceWork ReduceWork
	IsSuccess  bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
