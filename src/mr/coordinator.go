package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync/atomic"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	tasks     chan Work // a taskqueue
	timerMap  map[int]*time.Timer
	mapCount  int32
	reduCount int32
	nMap      int
	nReduce   int
}

func (c *Coordinator) CallGetWork(args *WorkArgs, reply *WorkReply) error {
	if len(c.tasks) == 0 {
		reply.HasWork = false
		return nil
	}
	tid := len(c.timerMap)
	c.timerMap[tid] = time.NewTimer(10 * time.Second)
	reply.Tid = tid
	reply.Work = <-c.tasks
	reply.HasWork = true

	go func() {
		<-c.timerMap[tid].C
		c.tasks <- reply.Work
	}()

	return nil
}

func (c *Coordinator) CallReport(args *ReportArgs, reply *ReportReply) error {
	if !c.timerMap[args.Tid].Stop() {
		reply.Success = false
		return nil
	}

	switch args.Work.WorkType {
	case MAP:
		if atomic.LoadInt32(&c.mapCount) == 0 {
			reply.Success = false
			return nil
		}
		if atomic.AddInt32(&c.mapCount, -1) == 0 {
			for i := 0; i < c.nReduce; i++ {
				c.tasks <- Work{
					WorkType:  REDUCE,
					FileIndex: i,
					NReduce:   c.nReduce,
					NMapWork:  c.nMap,
				}
				atomic.AddInt32(&c.reduCount, 1)
			}
		}
	case REDUCE:
		if atomic.LoadInt32(&c.reduCount) == 0 {
			reply.Success = false
			return nil
		}
	}
	reply.Success = true
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
	return atomic.LoadInt32(&c.mapCount) == 0 && atomic.LoadInt32(&c.reduCount) == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	var buflen int
	if len(files) > nReduce {
		buflen = len(files)
	} else {
		buflen = nReduce
	}

	c := Coordinator{
		nMap:      len(files),
		nReduce:   nReduce,
		mapCount:  0,
		reduCount: 0,
		tasks:     make(chan Work, buflen),
		timerMap:  make(map[int]*time.Timer),
	}

	for idx, file := range files {
		c.tasks <- Work{
			WorkType:  MAP,
			Filename:  file,
			FileIndex: idx,
			NReduce:   c.nReduce,
			NMapWork:  c.nMap,
		}
		atomic.AddInt32(&c.mapCount, 1)
	}

	c.server()

	return &c
}
