package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type WorkDetail struct {
	IsSuccess bool
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu         sync.Mutex      // an internal lock
	taskqueue  []Work          // a taskqueue
	reduceWork map[int]bool    // a record if reduceWork all success
	mapWork    map[string]bool // a record if  mapwork all success
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetWork(args *WorkArgs, reply *WorkReply) error {
	c.mu.Lock()
	if len(c.taskqueue) == 0 {
		reply.HasWork = false
		return nil
	}
	reply.HasWork = true
	reply.Work = c.taskqueue[0]
	c.taskqueue = c.taskqueue[1:]
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) ReplyFinish(args *WorkArgs, reply *WorkReply) error {
	c.mu.Lock()
	switch args.WorkType {
	case MAP:
		if args.IsSuccess {
			c.mapWork[args.MapWork.Filename] = true
		} else {
			c.taskqueue = append(c.taskqueue, Work{
				WorkType: MAP,
				MapWork:  args.MapWork,
			})
		}
	case REDUCE:
		if args.IsSuccess {
			c.reduceWork[args.ReduceWork.ReduceIndex] = true
		} else {
			c.taskqueue = append(c.taskqueue, Work{
				WorkType:   REDUCE,
				ReduceWork: args.ReduceWork,
			})
		}
	}
	c.mu.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.
	for _, v := range c.mapWork {
		ret = ret && v
	}

	for _, v := range c.reduceWork {
		ret = ret && v
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		taskqueue:  make([]Work, 0),
		reduceWork: make(map[int]bool),
		mapWork:    make(map[string]bool),
	}

	for idx, file := range files {
		c.mapWork[file] = false
		c.taskqueue = append(c.taskqueue, Work{
			WorkType: MAP,
			MapWork: MapWork{
				Filename:  file,
				FileIndex: idx,
				NReduce:   nReduce,
			},
		})
	}

	for i := 0; i < nReduce; i++ {
		c.reduceWork[i] = false
	}

	c.server()

	return &c
}
