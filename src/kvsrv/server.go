package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.

	return kv
}
