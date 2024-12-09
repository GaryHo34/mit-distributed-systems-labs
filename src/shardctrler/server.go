package shardctrler

import (
	"log"
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

var Debug = os.Getenv("DEBUG") == "6"

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	configs []*Config        // indexed by config num
	cache   map[int64]*Cache // client id -> seq
	chanmap map[int64]chan Done
}

type Op struct {
	Cmd interface{}
	ClientInfo
}

type Done struct {
	index int
	term  int
	conf  *Config
	err   Err
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	DPrintf("(Join)[%d] %+v\n", sc.me, args)
	r := sc.raft(args, args.Cid, args.Seq)
	reply.WrongLeader = r.WrongLeader
	reply.Err = r.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	DPrintf("(Leave)[%d] %+v\n", sc.me, args)
	r := sc.raft(args, args.Cid, args.Seq)
	reply.WrongLeader = r.WrongLeader
	reply.Err = r.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	DPrintf("(Move)[%d] %+v\n", sc.me, args)
	r := sc.raft(args, args.Cid, args.Seq)
	reply.WrongLeader = r.WrongLeader
	reply.Err = r.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	DPrintf("(Query)[%d] %+v\n", sc.me, args)
	r := sc.raft(args, args.Cid, args.Seq)
	reply.Conf = new(Config)
	reply.Conf.Copy(r.Conf)
	reply.WrongLeader = r.WrongLeader
	reply.Err = r.Err
}

func (sc *ShardCtrler) execJoin(servers map[int][]string) Err {
	DPrintf("(Join)[%d] servers %+v\n", sc.me, servers)
	oldConfig := sc.configs[len(sc.configs)-1]
	newConfig := new(Config)
	newConfig.Num = len(sc.configs)
	newConfig.Shards = [NShards]int{}
	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = oldConfig.Shards[i]
	}
	newConfig.Groups = make(map[int][]string)
	for gid, srvs := range oldConfig.Groups {
		newConfig.Groups[gid] = srvs
	}
	for gid, srvs := range servers {
		newConfig.Groups[gid] = srvs
	}
	sc.configs = append(sc.configs, newConfig)
	sc.rebalance()
	return OK
}

func (sc *ShardCtrler) execLeave(GIDs []int) Err {
	DPrintf("(Leave)[%d] GIDs %+v\n", sc.me, GIDs)
	gidMap := make(map[int]bool)
	for _, gid := range GIDs {
		gidMap[gid] = true
	}
	oldConfig := sc.configs[len(sc.configs)-1]
	newConfig := new(Config)
	newConfig.Num = len(sc.configs)
	newConfig.Shards = [NShards]int{}
	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = oldConfig.Shards[i]
	}
	newConfig.Groups = make(map[int][]string)
	for gid, srvs := range oldConfig.Groups {
		if _, ok := gidMap[gid]; !ok {
			newConfig.Groups[gid] = srvs
		}
	}
	sc.configs = append(sc.configs, newConfig)
	sc.rebalance()
	return OK
}

func (sc *ShardCtrler) execMove(Shard, GID int) Err {
	DPrintf("(Move)[%d] Shard %d GID %d\n", sc.me, Shard, GID)
	oldConfig := sc.configs[len(sc.configs)-1]
	newConfig := new(Config)
	newConfig.Num = len(sc.configs)
	newConfig.Shards = [NShards]int{}
	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = oldConfig.Shards[i]
	}
	newConfig.Shards[Shard] = GID
	newConfig.Groups = make(map[int][]string)
	for gid, srvs := range oldConfig.Groups {
		newConfig.Groups[gid] = srvs
	}
	sc.configs = append(sc.configs, newConfig)
	return OK
}

func (sc *ShardCtrler) execQuery(Num int, config *Config) Err {
	if Num < 0 || Num >= len(sc.configs) {
		config.Copy(sc.configs[len(sc.configs)-1])
	} else {
		config.Copy(sc.configs[Num])
	}
	DPrintf("(Query)[%d] Num %d, config %+v\n", sc.me, Num, config)
	return OK
}

func getChanId(term, index int) (id int64) {
	id = int64(term) << 32
	id += int64(index)
	return
}

func (sc *ShardCtrler) rebalance() {
	config := sc.configs[len(sc.configs)-1]
	serverCount := 0
	groupToShard := make(map[int][]int)
	groups := make([]struct {
		gid  int
		srvs []string
	}, 0)
	for gid, srvs := range config.Groups {
		serverCount += len(srvs)
		groupToShard[gid] = make([]int, 0)
		groups = append(groups, struct {
			gid  int
			srvs []string
		}{gid, srvs})
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].gid < groups[j].gid
	})
	if serverCount == 0 {
		return
	}
	groupShardCount := make(map[int]int)
	for _, g := range groups {
		gid := g.gid
		srvs := g.srvs
		groupShardCount[gid] = int(math.Floor(float64(NShards) * (float64(len(srvs)) / float64(serverCount))))
	}

	orphenShards := make([]int, 0)

	for i := 0; i < NShards; i++ {
		gid := config.Shards[i]
		if _, ok := config.Groups[gid]; ok {
			groupToShard[gid] = append(groupToShard[gid], i)
		} else {
			orphenShards = append(orphenShards, i)
		}
	}
	for _, g := range groups {
		gid := g.gid
		for len(groupToShard[gid]) > 1 && len(groupToShard[gid]) > groupShardCount[gid] {
			orphenShards = append(orphenShards, groupToShard[gid][len(groupToShard[gid])-1])
			groupToShard[gid] = groupToShard[gid][:len(groupToShard[gid])-1]
		}
	}

	for len(orphenShards) > 0 {
		lastOrphen := orphenShards[len(orphenShards)-1]
		orphenShards = orphenShards[:len(orphenShards)-1]
		isFound := false
		for _, g := range groups {
			gid := g.gid
			if len(groupToShard[gid])+1 <= groupShardCount[gid] {
				groupToShard[gid] = append(groupToShard[gid], lastOrphen)
				config.Shards[lastOrphen] = gid
				isFound = true
				break
			}
		}
		if isFound {
			continue
		}
		if _, ok := config.Groups[config.Shards[lastOrphen]]; ok {
			continue
		} else {
			for _, g := range groups {
				gid := g.gid
				if len(groupToShard[gid]) <= groupShardCount[gid] {
					groupToShard[gid] = append(groupToShard[gid], lastOrphen)
					config.Shards[lastOrphen] = gid
					break
				}
			}
		}
	}
}

func (sc *ShardCtrler) raft(cmd interface{}, cid int64, seq int) *Cache {
	ch := make(chan *Cache)
	go sc.startRaft(cmd, cid, seq, ch)
	r := <-ch
	close(ch)
	return r
}

func (sc *ShardCtrler) makeChan(term, index int) chan Done {
	id := getChanId(term, index)
	ch := make(chan Done, 1)
	sc.chanmap[id] = ch
	return ch
}

func (sc *ShardCtrler) closeAndDeleteChan(term, index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	id := getChanId(term, index)
	close(sc.chanmap[id])
	delete(sc.chanmap, id)
}

func (sc *ShardCtrler) isCacheHit(Cid int64, Seq int) (bool, *Cache) {
	if cache, ok := sc.cache[Cid]; ok && cache.Seq >= Seq {
		return true, cache
	} else if ok {
		return false, cache
	} else {
		sc.cache[Cid] = new(Cache)
		return false, sc.cache[Cid]
	}
}

func (sc *ShardCtrler) startRaft(cmd interface{}, cid int64, seq int, ch chan *Cache) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	rr := new(Cache)
	rr.Conf = new(Config)
	if hit, cache := sc.isCacheHit(cid, seq); hit {
		rr.Seq, rr.Err, rr.WrongLeader = cache.Seq, cache.Err, cache.WrongLeader
		rr.Conf.Copy(cache.Conf)
		ch <- rr
	} else {
		op := new(Op)
		op.Cmd, op.Cid, op.Seq = cmd, cid, seq
		index, term, isLeader := sc.rf.Start(op)
		if !isLeader {
			rr.WrongLeader = true
			rr.Err = OK
			ch <- rr
			return
		}
		donech := sc.makeChan(term, index)
		go sc.waitRaft(term, index, ch, donech)
		DPrintf("(startRaft) [%d] start raft with op %+v\n", sc.me, op)
	}
}

func (sc *ShardCtrler) waitRaft(term, index int, ch chan *Cache, donech chan Done) {
	timer := time.NewTimer(500 * time.Millisecond)
	rr := new(Cache)
	rr.Conf = new(Config)
	DPrintf("(waitRaft) [%d] wait for term: %d, index: %d\n", sc.me, term, index)
	select {
	case <-timer.C:
		DPrintf("(waitRaft) [%d] timeout, term: %d, index: %d\n", sc.me, term, index)
		rr.WrongLeader = true
		rr.Err = ""
		ch <- rr
	case done := <-donech:
		rr.WrongLeader = false
		rr.Conf.Copy(done.conf)
		rr.Err = done.err
		ch <- rr
	}
	sc.closeAndDeleteChan(term, index)
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) executor() {
	for !sc.killed() {
		msg := <-sc.applyCh
		DPrintf("(executor) [%d] receive msg %+v\n", sc.me, msg)
		sc.mu.Lock()
		if msg.CommandValid {
			DPrintf("(executor) [%d] type of command: %T\n", sc.me, msg.Command)
			op := msg.Command.(*Op)
			index, term, cid, seq := msg.CommandIndex, msg.CommandTerm, op.Cid, op.Seq
			hit, cache := sc.isCacheHit(cid, seq)
			if !hit {
				cache.Seq, cache.Conf, cache.Err = seq, new(Config), OK
				switch v := op.Cmd.(type) {
				case *JoinArgs:
					cache.Err = sc.execJoin(v.Srvs)
				case *LeaveArgs:
					cache.Err = sc.execLeave(v.Gids)
				case *MoveArgs:
					cache.Err = sc.execMove(v.Shard, v.Gid)
				case *QueryArgs:
					cache.Err = sc.execQuery(v.ConfNum, cache.Conf)
					DPrintf("(executor) [%d] query %+v\n", sc.me, cache.Conf)
				}
			}
			if ch, ok := sc.chanmap[getChanId(term, index)]; ok {
				config := new(Config)
				config.Copy(cache.Conf)
				select {
				case ch <- Done{index, term, config, cache.Err}:
				default:
				}
			}
		} else {
			log.Fatalf("Invalid applyMsg, %+v\n", msg)
		}
		sc.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(&Op{})
	labgob.Register(&JoinArgs{})
	labgob.Register(&LeaveArgs{})
	labgob.Register(&MoveArgs{})
	labgob.Register(&QueryArgs{})
	labgob.Register(&Config{})

	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]*Config, 0)
	sc.configs = append(sc.configs, new(Config))
	sc.configs[0].Groups = map[int][]string{}
	sc.configs[0].Shards = [NShards]int{}

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.cache = make(map[int64]*Cache)
	sc.chanmap = make(map[int64]chan Done)

	go sc.executor()

	return sc
}
