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

type WorkDetail struct {
	IsSuccess bool
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu         sync.Mutex            // an internal lock
	taskqueue  []Work                // a taskqueue
	reduceWork map[int]WorkStatus    // a record if reduceWork all success
	mapWork    map[string]WorkStatus // a record if  mapwork all success
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Timer(w Work) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	switch w.WorkType {
	case MAP:
		if c.mapWork[w.MapWork.Filename] != FINISHED {
			c.taskqueue = append(c.taskqueue, w)
			c.mapWork[w.MapWork.Filename] = NOTSTART
		}
	case REDUCE:
		if c.reduceWork[w.ReduceWork.ReduceIndex] != FINISHED {
			c.taskqueue = append(c.taskqueue, w)
			c.reduceWork[w.ReduceWork.ReduceIndex] = NOTSTART
		}
	}
	c.mu.Unlock()
}

func (c *Coordinator) CheckMapWork() {
	ret := true
	for _, v := range c.mapWork {
		ret = ret && (v == FINISHED)
	}
	if !ret {
		return
	}
	c.mu.Lock()
	fmt.Println("All map work finished")
	fmt.Println("Start reduce work")
	for i := 0; i < len(c.reduceWork); i++ {
		c.taskqueue = append(c.taskqueue, Work{
			WorkType: REDUCE,
			ReduceWork: ReduceWork{
				ReduceIndex: i,
				NMapWork:    len(c.mapWork),
			},
		})
	}
	c.mu.Unlock()
}

func (c *Coordinator) GetWork(args *WorkArgs, reply *WorkReply) error {
	if len(c.taskqueue) == 0 {
		reply.HasWork = false
		return nil
	}
	c.mu.Lock()
	w := c.taskqueue[0]
	c.taskqueue = c.taskqueue[1:]
	switch w.WorkType {
	case MAP:
		c.mapWork[w.MapWork.Filename] = STARTED
	case REDUCE:
		c.reduceWork[w.ReduceWork.ReduceIndex] = STARTED
	}
	c.mu.Unlock()

	reply.HasWork = true
	reply.Work = w

	go c.Timer(w)

	return nil
}

func (c *Coordinator) ReplyFinish(args *WorkArgs, reply *WorkReply) error {
	c.mu.Lock()
	switch args.WorkType {
	case MAP:
		if args.IsSuccess && c.mapWork[args.MapWork.Filename] == STARTED {
			c.mapWork[args.MapWork.Filename] = FINISHED
		} else {
			c.taskqueue = append(c.taskqueue, Work{
				WorkType: MAP,
				MapWork:  args.MapWork,
			})
			c.mapWork[args.MapWork.Filename] = NOTSTART
		}
	case REDUCE:
		if args.IsSuccess && c.mapWork[args.MapWork.Filename] == STARTED {
			c.reduceWork[args.ReduceWork.ReduceIndex] = FINISHED
		} else {
			c.taskqueue = append(c.taskqueue, Work{
				WorkType:   REDUCE,
				ReduceWork: args.ReduceWork,
			})
			c.mapWork[args.MapWork.Filename] = NOTSTART
		}
	}
	c.mu.Unlock()

	go c.CheckMapWork()

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
		ret = ret && (v == FINISHED)
	}

	for _, v := range c.reduceWork {
		ret = ret && (v == FINISHED)
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		taskqueue:  make([]Work, 0),
		reduceWork: make(map[int]WorkStatus),
		mapWork:    make(map[string]WorkStatus),
	}

	for idx, file := range files {
		c.mapWork[file] = NOTSTART
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
		c.reduceWork[i] = NOTSTART
	}

	c.server()

	return &c
}
