package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex  // an internal lock
	taskqueue   TaskQueue   // a taskqueue
	worktracker WorkTracker // a record if reduceWork all success
	done        bool
	nMap        int
	nReduce     int
}

type WorkTracker struct {
	statusmap map[Work]WorkStatus
	mu        sync.Mutex
}

func (w *WorkTracker) Set(work Work, status WorkStatus) {
	w.mu.Lock()
	w.statusmap[work] = status
	w.mu.Unlock()
}

func (w *WorkTracker) Get(work Work) WorkStatus {
	w.mu.Lock()
	status := w.statusmap[work]
	w.mu.Unlock()
	return status
}

func (w *WorkTracker) Done() bool {
	w.mu.Lock()
	ret := true
	for _, v := range w.statusmap {
		ret = ret && (v == FINISH)
	}
	w.mu.Unlock()
	return ret
}

/*task queue with thread lock*/
type TaskQueue struct {
	taskqueue []Work
	mu        sync.Mutex
}

func (t *TaskQueue) Push(w Work) {
	t.mu.Lock()
	t.taskqueue = append(t.taskqueue, w)
	t.mu.Unlock()
}

func (t *TaskQueue) Pop() Work {
	t.mu.Lock()
	w := t.taskqueue[0]
	t.taskqueue = t.taskqueue[1:]
	t.mu.Unlock()
	return w
}

func (t *TaskQueue) Empty() bool {
	var ret bool
	t.mu.Lock()
	ret = len(t.taskqueue) == 0
	t.mu.Unlock()
	return ret
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Timer(w Work, now time.Time) {
	for {
		if time.Since(now) > 10*time.Second {
			if c.worktracker.Get(w) == START {
				c.taskqueue.Push(w)
				c.worktracker.Set(w, IDLE)
			}
			return
		}
	}
}

func (c *Coordinator) CheckMapWork() {
	ret := c.worktracker.Done()

	if !ret {
		return
	}

	fmt.Println("All map work finished")
	fmt.Println("Start reduce work")

	for i := 0; i < c.nReduce; i++ {
		c.taskqueue.Push(Work{
			WorkID:    len(c.worktracker.statusmap),
			WorkType:  REDUCE,
			FileIndex: i,
			NReduce:   c.nReduce,
			NMapWork:  c.nMap,
		})
	}
}

func (c *Coordinator) CallGetWork(args *WorkRequest, reply *WorkResponse) error {
	if c.taskqueue.Empty() {
		reply.HasWork = false
		return nil
	}

	work := c.taskqueue.Pop()
	c.worktracker.Set(work, START)

	reply.HasWork = true
	reply.Work = work
	now := time.Now()

	go c.Timer(work, now)

	return nil
}

func (c *Coordinator) CallReport(args *ReportRequest, reply *ReportResponse) error {
	work := args.Work

	if c.worktracker.Get(work) == START {
		c.worktracker.Set(work, FINISH)
		if work.WorkType == MAP {
			c.mu.Lock()
			c.CheckMapWork()
			c.mu.Unlock()
		} else {
			c.mu.Lock()
			c.done = c.worktracker.Done()
			c.mu.Unlock()
		}
	}

	reply.IsSuccess = true

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

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:    len(files),
		nReduce: nReduce,
		done:    false,
		taskqueue: TaskQueue{
			taskqueue: []Work{},
		},
		worktracker: WorkTracker{
			statusmap: make(map[Work]WorkStatus),
		},
	}

	for idx, file := range files {
		work := Work{
			WorkID:    len(c.worktracker.statusmap),
			WorkType:  MAP,
			Filename:  file,
			FileIndex: idx,
			NReduce:   c.nReduce,
			NMapWork:  c.nMap,
		}
		c.taskqueue.Push(work)
		c.worktracker.Set(work, IDLE)
	}

	c.server()

	return &c
}
