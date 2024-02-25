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
	mu         sync.Mutex
	data       map[string]string
	appenTable map[int64](map[int]string)
	putTable   map[int64]int
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if val, ok := kv.data[args.Key]; ok {
		reply.Value = val
		return
	}
	reply.Value = ""
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if prevSeq, ok := kv.putTable[args.CId]; ok && prevSeq >= args.Seq {
		return
	}
	kv.putTable[args.CId] = args.Seq
	kv.data[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.appenTable[args.CId]; !ok {
		kv.appenTable[args.CId] = make(map[int]string)
	}
	if val, ok := kv.appenTable[args.CId][args.Seq]; ok {
		reply.Value = val
		return
	}
	kv.appenTable[args.CId][args.Seq] = kv.data[args.Key]
	reply.Value = kv.appenTable[args.CId][args.Seq]
	kv.data[args.Key] += args.Value
	delete(kv.appenTable[args.CId], args.Seq-1)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.appenTable = make(map[int64](map[int]string))
	kv.putTable = make(map[int64]int)
	return kv
}
