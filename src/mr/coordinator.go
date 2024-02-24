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

const TimerPoolSize = 10000

type Coordinator struct {
	// Your definitions here.
	tasks    chan Work // a taskqueue
	timerMap []*time.Timer
	eId      int
	wg       sync.WaitGroup
	nMap     int
	nReduce  int
	done     bool
}

func (c *Coordinator) CallGetWork(args *WorkArgs, reply *WorkReply) error {
	if len(c.tasks) == 0 {
		reply.HasWork = false
		return nil
	}
	id := c.eId
	c.eId++
	c.timerMap[id%TimerPoolSize] = time.NewTimer(10 * time.Second)
	reply.Tid = id
	reply.Work = <-c.tasks
	reply.HasWork = true

	go func() {
		<-c.timerMap[id%TimerPoolSize].C
		c.tasks <- reply.Work
	}()

	return nil
}

func (c *Coordinator) CallReport(args *ReportArgs, reply *ReportReply) error {
	if !c.timerMap[args.Tid%TimerPoolSize].Stop() {
		reply.Success = false
		return nil
	}
	c.wg.Done()
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
	return c.done
}

func StartReduceWork(c *Coordinator) {
	c.wg.Wait()
	for i := 0; i < c.nReduce; i++ {
		c.tasks <- Work{
			WorkType:  REDUCE,
			FileIndex: i,
			NReduce:   c.nReduce,
			NMapWork:  c.nMap,
		}
		c.wg.Add(1)
	}
	go WorkDone(c)
}

func WorkDone(c *Coordinator) {
	c.wg.Wait()
	c.done = true
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
		nMap:     len(files),
		nReduce:  nReduce,
		wg:       sync.WaitGroup{},
		tasks:    make(chan Work, buflen),
		timerMap: make([]*time.Timer, TimerPoolSize),
		done:     false,
	}

	for idx, file := range files {
		c.tasks <- Work{
			WorkType:  MAP,
			Filename:  file,
			FileIndex: idx,
			NReduce:   c.nReduce,
			NMapWork:  c.nMap,
		}
		c.wg.Add(1)
	}
	go StartReduceWork(&c)
	c.server()

	return &c
}
