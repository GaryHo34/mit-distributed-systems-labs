package kvraft

import (
	"bytes"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

var Debug = os.Getenv("DEBUG") == "1"

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Opcode int

const (
	GET Opcode = iota
	PUT
	APPEND
)

type Op struct {
	Op    Opcode
	Key   string
	Value string
	ClientInfo
}

type Done struct {
	index int
	term  int
	value string
	err   Err
}

func NewOp(op Opcode, key, value string, clientId int64, seq int) Op {
	return Op{op, key, value, ClientInfo{clientId, seq}}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	ps      *raft.Persister
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	data    map[string]string
	cache   map[int64]*RaftReply // client id -> seq
	chanmap map[int64]chan Done
}

func getChanId(term, index int) (id int64) {
	id = int64(term) << 32
	id += int64(index)
	return
}

func (kv *KVServer) makeChan(term, index int) chan Done {
	id := getChanId(term, index)
	ch := make(chan Done, 1)
	kv.chanmap[id] = ch
	return ch
}

func (kv *KVServer) closeAndDeleteChan(term, index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	id := getChanId(term, index)
	close(kv.chanmap[id])
	delete(kv.chanmap, id)
}

func (kv *KVServer) isCacheHit(clientId int64, seqNum int) (bool, *RaftReply) {
	// Why cache.SeqNum >= seqNum works?
	// 1. If the seq of cache equals to seqNum, it means the operation has been
	//    executed. Return the value directly.
	// 2. If the seq of cache is Greater than seqNum, it means some operations
	//    after this Op have been executed, which implies client has already
	//    received the result of this Op (the operation must be completed before
	//	  next operation happened). Theorically, return anything is OK.
	if cache, ok := kv.cache[clientId]; ok && cache.SeqNum >= seqNum {
		return true, cache
	}
	rr := new(RaftReply)
	rr.Value = ""
	rr.Err = ErrWrongLeader
	return false, rr
}

func (kv *KVServer) encode() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.cache)
	e.Encode(kv.data)
	return w.Bytes()
}

func (kv *KVServer) decode(buf []byte) {
	if buf == nil || len(buf) < 1 {
		return
	}
	r := bytes.NewBuffer(buf)
	d := labgob.NewDecoder(r)
	var cache map[int64]*RaftReply
	var data map[string]string
	if d.Decode(&cache) != nil || d.Decode(&data) != nil {
		log.Fatal("Decode error")
		return
	}
	kv.cache = cache
	kv.data = data
}

func (kv *KVServer) startRaft(k, v string, op Opcode, cid int64, seq int, ch chan *RaftReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if hit, cache := kv.isCacheHit(cid, seq); hit {
		ch <- cache
		return
	}
	DPrintf("(startRaft) [%d] start raft, op: %v, key: %s, value: %s, cid: %d, seq: %d\n", kv.me, op, k, v, cid, seq)
	index, term, isLeader := kv.rf.Start(NewOp(op, k, v, cid, seq))
	if !isLeader {
		rr := new(RaftReply)
		rr.Value = ""
		rr.Err = ErrWrongLeader
		ch <- rr
		return
	}
	donech := kv.makeChan(term, index)
	go kv.waitRaft(term, index, ch, donech)
}

func (kv *KVServer) waitRaft(term, index int, ch chan *RaftReply, donech chan Done) {
	timer := time.NewTimer(500 * time.Millisecond)
	rr := new(RaftReply)
	DPrintf("(startRaft) [%d] wait for term: %d, index: %d\n", kv.me, term, index)
	select {
	case <-timer.C:
		DPrintf("(startRaft) [%d] timeout, term: %d, index: %d\n", kv.me, term, index)
		rr.Value = ""
		rr.Err = ErrWrongLeader
		ch <- rr
	case done := <-donech:
		rr.Value = done.value
		rr.Err = done.err
		ch <- rr
	}
	kv.closeAndDeleteChan(term, index)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	ch := make(chan *RaftReply)
	go kv.startRaft(args.Key, "", GET, args.ClientId, args.SeqNum, ch)
	r := <-ch
	reply.Value = r.Value
	reply.Err = r.Err
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	var op Opcode
	if args.OpStr == "Put" {
		op = PUT
	} else {
		op = APPEND
	}
	ch := make(chan *RaftReply)
	go kv.startRaft(args.Key, args.Value, op, args.ClientId, args.SeqNum, ch)
	r := <-ch
	reply.Err = r.Err
}

// Serializes the execution of operations on the key-value store.
func (kv *KVServer) executor() {
	for !kv.killed() {
		var index, term int
		msg := <-kv.applyCh
		DPrintf("(executor) [%d] receive msg %+v\n", kv.me, msg)
		kv.mu.Lock()
		if msg.CommandValid {
			index, term = msg.CommandIndex, msg.CommandTerm
			op := msg.Command.(Op)
			code, key, value, clientId, seq := op.Op, op.Key, op.Value, op.ClientId, op.SeqNum
			var err Err
			if hit, cache := kv.isCacheHit(clientId, seq); hit {
				err = cache.Err
				value = cache.Value
			} else {
				switch code {
				case GET:
					DPrintf("(executor) [%d] get %s: %s\n", kv.me, key, kv.data[key])
					if val, ok := kv.data[key]; ok {
						value = val
						err = OK
					} else {
						value = ""
						err = ErrNoKey
					}
				case PUT:
					DPrintf("(executor) [%d] put %s: %s\n", kv.me, key, value)
					kv.data[key] = value
					err = OK
				case APPEND:
					kv.data[key] += value
					DPrintf("(executor) [%d] append %s: %s\n", kv.me, key, kv.data[key])
					err = OK
				}
				if _, ok := kv.cache[clientId]; !ok {
					kv.cache[clientId] = new(RaftReply)
				}
				kv.cache[clientId].SeqNum = seq
				kv.cache[clientId].Value = value
				kv.cache[clientId].Err = err
				if kv.maxraftstate != -1 && kv.maxraftstate < kv.ps.RaftStateSize() {
					kv.rf.Snapshot(index, kv.encode())
				}
			}
			if ch, ok := kv.chanmap[getChanId(term, index)]; ok {
				select {
				case ch <- Done{index, term, value, err}:
				default:
				}
			}
		} else if msg.SnapshotValid {
			kv.decode(msg.Snapshot)
		} else {
			log.Fatalf("Invalid applyMsg, %+v\n", msg)
		}
		kv.mu.Unlock()
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.ps = persister
	kv.data = make(map[string]string)
	kv.cache = make(map[int64]*RaftReply)
	kv.chanmap = make(map[int64]chan Done)

	// Read from persister if any
	kv.decode(kv.ps.ReadSnapshot())

	go kv.executor()

	return kv
}
