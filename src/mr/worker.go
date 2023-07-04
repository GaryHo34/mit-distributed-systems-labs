package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// a while(true) loop in go
	for {
		work := CallGetWok()

		if !work.HasWork {
			//sleep for a 3 seconds
			time.Sleep(3 * time.Second)
			continue
		}

		if work.Work.WorkType == MAP {
			DoMapWork(work.Work.MapWork, mapf)
		}
	}
}

func DoMapWork(work MapWork, mapf func(string, string) []KeyValue) {
	filename := work.Filename
	fmt.Println("DoMapWork: ", filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := ioutil.ReadAll(file)

	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	file.Close()

	kva := mapf(work.Filename, string(content))

	//make a
	for i := 0; i < work.NReduce; i++ {
		imtFilename := fmt.Sprintf("mr-%d-%d", work.FileIndex, i)

		imtFile, err := ioutil.TempFile(".", imtFilename)

		if err != nil {
			log.Fatalf("cannot create %v", imtFilename)
		}

		for _, kv := range kva {
			hash := ihash(kv.Key) % work.NReduce
			if hash == i {
				fmt.Fprintf(imtFile, "%v %v\n", kv.Key, kv.Value)
			}
		}

		imtFile.Close()

		os.Rename(imtFile.Name(), imtFilename)
	}

	CallReplyFinish(Work{
		WorkType: MAP,
		MapWork:  work,
	})
}

func CallReplyFinish(w Work) WorkReply {
	args := WorkArgs{}
	reply := WorkReply{}

	args.WorkType = w.WorkType

	if w.WorkType == MAP {
		args.MapWork = w.MapWork
	} else {
		args.ReduceWork = w.ReduceWork
	}

	args.IsSuccess = true

	ok := call("Coordinator.ReplyFinish", &args, &reply)

	if !ok {
		fmt.Printf("call failed!\n")
	}

	return reply
}

func CallGetWok() WorkReply {
	args := WorkArgs{}
	reply := WorkReply{}
	ok := call("Coordinator.GetWork", &args, &reply)

	if !ok {
		fmt.Printf("call failed!\n")
	}

	return reply
}

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
