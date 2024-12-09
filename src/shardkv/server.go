package shardkv

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
	"6.5840/shardctrler"
)

var Debug = os.Getenv("DEBUG") == "5"

func DPrintf(format string, a ...interface{}) (n int, err error) {
	//s := fmt.Sprintf(format, a...)
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
	CONFIG
	SHARDMOVE
)

type Op struct {
	Cmd interface{}
	ClientInfo
}

type Done struct {
	index int
	term  int
	value string
	err   Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	ps           *raft.Persister
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	maxraftstate int // snapshot if log grows this big
	ck           *shardctrler.Clerk
	config       shardctrler.Config

	shards         [shardctrler.NShards]ShardData
	shardPushQueue [shardctrler.NShards]([]*ShardPushTask)
	chanmap        map[int64]chan Done
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	DPrintf("(Get) [%d, %d] receive Get %+v\n", kv.me, kv.gid, args)
	r := kv.raft(args, args.Cid, args.Seq)
	reply.Value = r.Value
	reply.Err = r.Err
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("(PutAppend) [%d, %d] receive %s %+v\n", kv.me, kv.gid, args.OpStr, args)
	r := kv.raft(args, args.Cid, args.Seq)
	reply.Err = r.Err
}

// This is a private raft interface for ShardKV to use.
func (kv *ShardKV) updateConfig(args *ConfUpdateArgs) Err {
	DPrintf("(updateConfig) [%d, %d] raft config %+v\n", kv.me, kv.gid, args)
	r := kv.raft(args, args.Cid, args.Seq)
	return r.Err
}

// This is a RPC handler for ShardKV to use.
func (kv *ShardKV) PushShard(args *ShardPushTaskArgs, reply *ShardPushTaskReply) {
	DPrintf("(PushShard) [%d, %d] received PushShard %+v\n", kv.me, kv.gid, args)
	r := kv.raft(args, args.Cid, args.Seq)
	reply.Err = r.Err
}

func getChanId(term, index int) (id int64) {
	id = int64(term) << 32
	id += int64(index)
	return
}

func (kv *ShardKV) makeChan(term, index int) chan Done {
	id := getChanId(term, index)
	ch := make(chan Done, 1)
	kv.chanmap[id] = ch
	return ch
}

func (kv *ShardKV) closeAndDeleteChan(term, index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	id := getChanId(term, index)
	close(kv.chanmap[id])
	delete(kv.chanmap, id)
}

func (kv *ShardKV) isShardCacheHit(cid int64, seq, shard int) (bool, *Cache) {
	if cache, ok := kv.shards[shard].Cache[cid]; ok && cache.Seq >= seq {
		DPrintf("(isCacheHit) [%d, %d] cache hit %+v\n", kv.me, kv.gid, cache)
		return true, cache
	} else if ok {
		return false, cache
	} else {
		kv.shards[shard].Cache[cid] = new(Cache)
		return false, kv.shards[shard].Cache[cid]
	}
}

func (kv *ShardKV) startRaft(cmd interface{}, cid int64, seq int, ch chan *Cache) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	rr := new(Cache)
	op := new(Op)
	op.Cmd, op.Cid, op.Seq = cmd, cid, seq
	// We remove the cache check here because the cases are too complicated
	// The cache check is moved to the executor
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		rr.Err = ErrWrongLeader
		ch <- rr
		return
	}
	donech := kv.makeChan(term, index)
	go kv.waitRaft(term, index, ch, donech)
	DPrintf("(startRaft) [%d, %d] start raft index: %d, term: %d, with op %+v\n", kv.me, kv.gid, index, term, op.Cmd)
}

func (kv *ShardKV) waitRaft(term, index int, ch chan *Cache, donech chan Done) {
	timer := time.NewTimer(500 * time.Millisecond)
	rr := new(Cache)
	DPrintf("(waitRaft) [%d, %d] wait for term: %d, index: %d\n", kv.me, kv.gid, term, index)
	select {
	case <-timer.C:
		DPrintf("(waitRaft) [%d, %d] timeout, term: %d, index: %d\n", kv.me, kv.gid, term, index)
		rr.Err = ErrWrongGroup
	case done := <-donech:
		rr.Value = done.value
		rr.Err = done.err
	}
	ch <- rr
	kv.closeAndDeleteChan(term, index)
}

func (kv *ShardKV) raft(cmd interface{}, cid int64, seq int) *Cache {
	ch := make(chan *Cache)
	go kv.startRaft(cmd, cid, seq, ch)
	r := <-ch
	close(ch)
	return r
}

func (kv *ShardKV) encode() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.shards)
	e.Encode(kv.config)
	e.Encode(kv.shardPushQueue)

	return w.Bytes()
}

func (kv *ShardKV) decode(buf []byte) {
	if buf == nil || len(buf) < 1 {
		return
	}
	r := bytes.NewBuffer(buf)
	d := labgob.NewDecoder(r)

	var shards [shardctrler.NShards]ShardData
	var config shardctrler.Config
	var shardPushQueue [shardctrler.NShards][]*ShardPushTask

	if d.Decode(&shards) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&shardPushQueue) != nil {
		log.Fatal("Decode error")
		return
	}
	kv.shards = shards
	for i := 0; i < shardctrler.NShards; i++ {
		kv.shardPushQueue[i] = append([]*ShardPushTask{}, shardPushQueue[i]...)
	}
	kv.config.Copy(&config)
}

func (kv *ShardKV) dataMigration(newConfig *shardctrler.Config) {
	DPrintf("(dataMigration) [%d, %d] start data migration %+v\n", kv.me, kv.gid, newConfig)
	migrationTasks := [shardctrler.NShards]*ShardPushTask{}
	if kv.config.Num == 0 { // initial config update, set
		for shard := range newConfig.Shards {
			kv.shards[shard].IsReady = true
		}
	} else {
		for shard, newGid := range newConfig.Shards {
			oldGid := kv.config.Shards[shard]
			if oldGid != kv.gid && newGid == kv.gid {
				// Case 1 (newGid == kv.gid): Waiting for data from other group
				kv.shards[shard].IsReady = false
			} else if oldGid == kv.gid && newGid != kv.gid {
				// Case 2 (newGid != kv.gid): Set shard to not ready
				// Case 3 (oldGid == kv.gid && newGid != kv.gid): Start to push data to other group
				newTask := new(ShardPushTask)
				newTask.ConfNum = newConfig.Num
				newTask.Gid = newGid
				newTask.Servers = append([]string{}, newConfig.Groups[newGid]...)
				newTask.Shard = shard
				newTask.ShardData = ShardData{
					Data:    make(map[string]string),
					Cache:   make(map[int64]*Cache),
					IsReady: true,
				}
				for k, v := range kv.shards[shard].Data {
					newTask.ShardData.Data[k] = v
				}
				for cid, cache := range kv.shards[shard].Cache {
					newTask.ShardData.Cache[cid] = new(Cache)
					newTask.ShardData.Cache[cid].Copy(cache)
				}
				migrationTasks[shard] = newTask
				// Clear shard data
				kv.shards[shard].Data = make(map[string]string)
				kv.shards[shard].Cache = make(map[int64]*Cache)
				kv.shards[shard].IsReady = true
			} else {
				// Case 4 (oldGid == kv.gid && newGid == kv.gid): Set shard to ready
				kv.shards[shard].IsReady = true
			}
		}
	}
	kv.config.Copy(newConfig)
	DPrintf("(dataMigration) [%d, %d] migrationTasks %+v\n", kv.me, kv.gid, migrationTasks)
	for _, task := range migrationTasks {
		if task != nil {
			kv.shardPushQueue[task.Shard] = append(kv.shardPushQueue[task.Shard], task)
		}
	}
}

// Serializes the execution of operations on the key-value store.
func (kv *ShardKV) executor() {
	for !kv.killed() {
		msg := <-kv.applyCh
		kv.mu.Lock()
		if msg.CommandValid {
			op := msg.Command.(*Op)
			value := ""
			var err Err
			err = OK
			DPrintf("(executor) [%d, %d] receive msg %+v, type of command: %T\n", kv.me, kv.gid, msg, op.Cmd)
			index, term, cid, seq := msg.CommandIndex, msg.CommandTerm, op.Cid, op.Seq
			switch v := op.Cmd.(type) {
			case *GetArgs:
				k := v.Key
				if kv.config.Shards[key2shard(k)] != kv.gid {
					// Case: Wrong Group
					DPrintf("(executor.GetArgs) [%d, %d] wrong group %d\n", kv.me, kv.gid, kv.config.Shards[key2shard(k)])
					err = ErrWrongGroup
				} else if ok, cache := kv.isShardCacheHit(cid, seq, key2shard(k)); ok {
					// Case: Cache is found and Seq is less than or equal to the cache's Seq
					DPrintf("(executor.GetArgs) [%d, %d] cache hit %+v\n", kv.me, kv.gid, cache)
					value = cache.Value
					err = cache.Err
				} else if !kv.shards[key2shard(k)].IsReady {
					// Case: Shard is not ready
					DPrintf("(executor.GetArgs) [%d, %d] shard not ready %d\n", kv.me, kv.gid, key2shard(k))
					err = ErrWrongGroup
				} else {
					if val, ok := kv.shards[key2shard(k)].Data[k]; ok {
						// Case: Shard is ready and key is found
						DPrintf("(executor.GetArgs) [%d, %d] get %s: %s\n", kv.me, kv.gid, k, kv.shards[key2shard(k)].Data[k])
						value = val
					} else {
						// Case: Shard is ready and key is not found
						DPrintf("(executor.GetArgs) [%d, %d] key error %s\n", kv.me, kv.gid, k)
						err = ErrNoKey
					}
					cache.Value = value
					cache.Err = err
					cache.Seq = seq
				}
			case *PutAppendArgs:
				k, val := v.Key, v.Value
				if kv.config.Shards[key2shard(k)] != kv.gid {
					// Case: Wrong Group
					DPrintf("(executor.PutAppendArgs) [%d, %d] wrong group %d\n", kv.me, kv.gid, kv.config.Shards[key2shard(k)])
					err = ErrWrongGroup
				} else if ok, cache := kv.isShardCacheHit(cid, seq, key2shard(k)); ok {
					// Case: Cache is found and Seq is less than or equal to the cache's Seq
					DPrintf("(executor.PutAppendArgs) [%d, %d] cache hit %+v\n", kv.me, kv.gid, cache)
					err = cache.Err
				} else if !kv.shards[key2shard(k)].IsReady {
					// Case: Shard is not ready
					DPrintf("(executor.GetArgs) [%d, %d] shard not ready %d\n", kv.me, kv.gid, key2shard(k))
					err = ErrWrongGroup
				} else {
					if v.OpStr == "Put" {
						kv.shards[key2shard(k)].Data[k] = val
						DPrintf("(executor.PutAppendArgs) [%d, %d] Put success %s: %s\n", kv.me, kv.gid, k, kv.shards[key2shard(k)].Data[k])
					} else if v.OpStr == "Append" {
						tmp := kv.shards[key2shard(k)].Data[k]
						kv.shards[key2shard(k)].Data[k] += val
						DPrintf("(executor.PutAppendArgs) [%d, %d] key: %s, Append %s to %s success result: %s\n", kv.me, kv.gid, k, val, tmp, kv.shards[key2shard(k)].Data[k])
					}
					cache.Err = err
					cache.Seq = seq
				}
			case *ConfUpdateArgs:
				if kv.config.Num+1 == v.Conf.Num {
					DPrintf("(executor.ConfUpdateArgs) [%d, %d] start update config to %+v\n", kv.me, kv.gid, v.Conf)
					kv.dataMigration(v.Conf)
				} else {
					DPrintf("(executor.ConfUpdateArgs) [%d, %d] config is invalid, current: %d, received: %d\n", kv.me, kv.gid, kv.config.Num, v.Conf.Num)
				}
			case *ShardPushTaskArgs:
				if v.Task.ConfNum > kv.config.Num {
					DPrintf("(executor.ShardPushTaskArgs) [%d, %d] shard migration Task %+v is invalid\n", kv.me, kv.gid, v)
					err = ErrWrongGroup
				} else if v.Task.ConfNum == kv.config.Num && v.Task.Gid == kv.gid && !kv.shards[v.Task.Shard].IsReady {
					DPrintf("(executor.ShardPushTaskArgs) [%d, %d] update shard data from other groups %+v\n", kv.me, kv.gid, v)
					for kk, vv := range v.Task.ShardData.Data {
						kv.shards[v.Task.Shard].Data[kk] = vv
					}
					for cid, cache := range v.Task.ShardData.Cache {
						kv.shards[v.Task.Shard].Cache[cid] = new(Cache)
						kv.shards[v.Task.Shard].Cache[cid].Copy(cache)
					}
					kv.shards[v.Task.Shard].IsReady = true
				}
			case *ShardPushFinishArgs:
				// Agree the shard push task has been finished,
				// remove the task from the queue
				gid, shard := v.Gid, v.Shard
				if len(kv.shardPushQueue[shard]) > 0 &&
					kv.shardPushQueue[shard][0].Gid == gid &&
					kv.shardPushQueue[shard][0].Shard == shard {
					kv.shardPushQueue[shard] = append([]*ShardPushTask{}, kv.shardPushQueue[shard][1:]...)
				}
			}
			if kv.maxraftstate != -1 && kv.maxraftstate < kv.ps.RaftStateSize() {
				kv.rf.Snapshot(index, kv.encode())
			}
			if ch, ok := kv.chanmap[getChanId(term, index)]; ok {
				DPrintf("(executor.Done) [%d, %d] send Done to ch, term: %d, index: %d\n", kv.me, kv.gid, term, index)
				select {
				case ch <- Done{index, term, value, err}:
				default:
				}
			}
		} else if msg.SnapshotValid {
			DPrintf("(executor.Snapshot) [%d, %d] receive snapshot %+v\n", kv.me, kv.gid, msg)
			kv.decode(msg.Snapshot)
		} else {
			log.Fatalf("Invalid applyMsg, %+v\n", msg)
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) isShardAllReady() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, shard := range kv.shards {
		if !shard.IsReady {
			return false
		}
	}
	return true
}

func (kv *ShardKV) configPuller() {
	for !kv.killed() {
		if !kv.isShardAllReady() {
			DPrintf("(configPuller) [%d, %d] shard is not ready\n", kv.me, kv.gid)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		confNum := kv.config.Num
		kv.mu.Unlock()
		conf := kv.ck.Query(confNum + 1)
		if conf.Num == confNum+1 {
			kv.mu.Lock()
			args := new(ConfUpdateArgs)
			args.Conf = &conf
			go kv.updateConfig(args)
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) pushShard(args *ShardPushTaskArgs, shard int) {
	DPrintf("(pushShard) [%d, %d] start push shard %+v\n", kv.me, kv.gid, args)
	for si := 0; si < len(args.Task.Servers); si++ {
		srv := kv.make_end(args.Task.Servers[si])
		reply := new(ShardPushTaskReply)
		ok := srv.Call("ShardKV.PushShard", args, reply)
		if ok && reply.Err == OK { // Success
			DPrintf("(pushShard) [%d, %d] push shard %+v to %s success\n", kv.me, kv.gid, args, args.Task.Servers[si])
			finish := new(ShardPushFinishArgs)
			finish.Shard = shard
			finish.Gid = args.Task.Gid
			kv.raft(finish, 0, 0)
			return
		}
	}
}

// Each shard will have a pusher to push data to other group, after success,
// start a raft to make all followers agree on the result
func (kv *ShardKV) shardPusher(shard int) {
	for !kv.killed() {
		kv.mu.Lock()
		if len(kv.shardPushQueue[shard]) == 0 {
			kv.mu.Unlock()
		} else {
			// start to push shard
			args := new(ShardPushTaskArgs)
			args.Task = kv.shardPushQueue[shard][0]
			kv.mu.Unlock()
			kv.pushShard(args, shard)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	DPrintf("(StartServer) [%d, %d] start server\n", me, gid)
	labgob.Register(&Op{})
	labgob.Register(&GetArgs{})
	labgob.Register(&PutAppendArgs{})
	labgob.Register(&ConfUpdateArgs{})
	labgob.Register(&Cache{})
	labgob.Register(&ShardData{})
	labgob.Register(&ShardPushTask{})
	labgob.Register(&ShardPushTaskArgs{})
	labgob.Register(&ShardPushFinishArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ck = shardctrler.MakeClerk(ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.config = shardctrler.Config{}
	kv.config.Num = 0
	kv.config.Shards = [shardctrler.NShards]int{}
	kv.config.Groups = make(map[int][]string)

	kv.ps = persister
	kv.chanmap = make(map[int64]chan Done)

	for i := 0; i < shardctrler.NShards; i++ {
		kv.shards[i] = ShardData{
			Data:    make(map[string]string),
			Cache:   make(map[int64]*Cache),
			IsReady: true,
		}
		kv.shardPushQueue[i] = make([]*ShardPushTask, 0)
	}

	// Read from persister if any
	kv.decode(kv.ps.ReadSnapshot())

	for i := 0; i < shardctrler.NShards; i++ {
		go kv.shardPusher(i)
	}
	go kv.executor()
	go kv.configPuller()

	return kv
}
