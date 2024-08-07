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

type Op struct {
	Op       Opc
	Key      string
	Value    string
	ClientId int64
	Seq      int
	Chanid   int64
}

type Done struct {
	index int
	term  int
	value string
	err   Err
}

type Cache struct {
	Seq   int
	Value string
	Err   Err
}

func (kv *KVServer) newChan() int64 {
	id := nrand()
	for _, exist := kv.chanmap.Load(id); exist; _, exist = kv.chanmap.Load(id) {
		id = nrand()
	}
	kv.chanmap.Store(id, make(chan Done))
	return id
}

func NewOp(op Opc, key, value string, clientId int64, seq int, chanid int64) Op {
	return Op{
		Op:       op,
		Key:      key,
		Value:    value,
		ClientId: clientId,
		Seq:      seq,
		Chanid:   chanid,
	}
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
	cache   map[int64]*Cache // client id -> seq
	chanmap sync.Map
}

func (kv *KVServer) closeAndDeleteChan(chanid int64) {
	ach, _ := kv.chanmap.Load(chanid)
	ch := ach.(chan Done)
	close(ch)
	kv.chanmap.Delete(chanid)
}

func (kv *KVServer) waitForDone(chanid int64, index, term int) (v string, e Err) {
	ach, _ := kv.chanmap.Load(chanid)
	ch := ach.(chan Done)
	timer := time.NewTimer(3 * time.Second)
	select {
	case <-timer.C:
		v = ""
		e = ErrWrongLeader
	case done := <-ch:
		if done.index != index || done.term != term {
			v = ""
			e = ErrWrongLeader
		} else {
			v = done.value
			e = done.err
		}
	}
	kv.closeAndDeleteChan(chanid)
	return
}

func (kv *KVServer) isCacheHit(clientId int64, seqNum int) (bool, string, Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ca, ok := kv.cache[clientId]
	if ok && ca.Seq >= seqNum {
		return true, ca.Value, ca.Err
	}
	return false, "", ErrWrongLeader
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
	var cache map[int64]*Cache
	var data map[string]string
	if d.Decode(&cache) != nil || d.Decode(&data) != nil {
		log.Fatal("Decode error")
		return
	}
	kv.cache = cache
	kv.data = data
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	k, clientId, seqNum := args.Key, args.ClientId, args.SeqNum
	// If we can find a seq greater or equal to seqNum in the cache,
	// it means the raft log has already been executed.
	// We can return the value directly.
	if hit, v, e := kv.isCacheHit(clientId, seqNum); hit {
		reply.Value = v
		reply.Err = e
		return
	}
	chanid := kv.newChan()
	index, term, isLeader := kv.rf.Start(NewOp(GET, k, "", clientId, seqNum, chanid))
	if !isLeader {
		kv.closeAndDeleteChan(chanid)
		reply.Err = ErrWrongLeader
		return
	}
	value, err := kv.waitForDone(chanid, index, term)
	reply.Value = value
	reply.Err = err
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	var code Opc
	if args.Op == "Put" {
		code = PUT
	} else {
		code = APPEND
	}
	k, v, clientId, seqNum := args.Key, args.Value, args.ClientId, args.SeqNum
	if hit, _, e := kv.isCacheHit(clientId, seqNum); hit {
		reply.Err = e
		return
	}
	chanid := kv.newChan()
	index, term, isLeader := kv.rf.Start(NewOp(code, k, v, clientId, seqNum, chanid))
	if !isLeader {
		kv.closeAndDeleteChan(chanid)
		reply.Err = ErrWrongLeader
		return
	}
	_, err := kv.waitForDone(chanid, index, term)
	reply.Err = err
}

// Serializes the execution of operations on the key-value store.
func (kv *KVServer) executor() {
	for !kv.killed() {
		var index, term int
		msg := <-kv.applyCh
		if msg.CommandValid {
			if msg.Command == nil {
				log.Panicf("Invalid applyMsg %+v\n", msg)
				continue
			}
			index, term = msg.CommandIndex, msg.CommandTerm
			op := msg.Command.(Op)
			code, k, v, clientId, seq, chanid := op.Op, op.Key, op.Value, op.ClientId, op.Seq, op.Chanid
			var err Err
			if hit, pv, pe := kv.isCacheHit(clientId, seq); hit {
				err = pe
				v = pv
			} else {
				kv.mu.Lock()
				switch code {
				case GET:
					tv, ok := kv.data[k]
					if !ok {
						v = ""
						err = ErrNoKey
					} else {
						v = tv
						err = OK
					}
				case PUT:
					kv.data[k] = v
					err = OK
				case APPEND:
					kv.data[k] += v
					err = OK
				}
				if _, ok := kv.cache[clientId]; !ok {
					kv.cache[clientId] = new(Cache)
				}
				kv.cache[clientId].Seq = seq
				kv.cache[clientId].Value = v
				kv.cache[clientId].Err = err
				if kv.maxraftstate != -1 && kv.maxraftstate < kv.ps.RaftStateSize() {
					kv.rf.Snapshot(index, kv.encode())
				}
				kv.mu.Unlock()
			}
			if ch, ok := kv.chanmap.Load(chanid); ok {
				ch := ch.(chan Done)
				select {
				case ch <- Done{index, term, v, err}:
				default:
				}
			}
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			kv.decode(msg.Snapshot)
			kv.mu.Unlock()
		} else {
			log.Fatalf("Invalid applyMsg, %+v\n", msg)
		}
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
	kv.cache = make(map[int64]*Cache)

	// Read from persister if any
	kv.decode(kv.ps.ReadSnapshot())

	go kv.executor()

	return kv
}
