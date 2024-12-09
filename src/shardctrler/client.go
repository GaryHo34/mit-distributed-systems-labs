package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	cid     int64
	seq     int
	leader  int32 // cache the leader
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.cid = nrand()
	ck.seq = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := new(QueryArgs)
	args.ConfNum, args.Cid, args.Seq = num, ck.cid, ck.seq
	ck.seq++

	leader := int(atomic.LoadInt32(&ck.leader))
	for {
		for i := 0; i < len(ck.servers); i++ {
			peer := (leader + i) % len(ck.servers)
			reply := new(QueryReply)
			ok := ck.servers[peer].Call("ShardCtrler.Query", args, reply)
			if ok && !reply.WrongLeader {
				conf := new(Config)
				conf.Copy(reply.Conf)
				return *conf
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := new(JoinArgs)
	args.Srvs, args.Cid, args.Seq = servers, ck.cid, ck.seq
	ck.seq++

	leader := int(atomic.LoadInt32(&ck.leader))
	for {
		for i := 0; i < len(ck.servers); i++ {
			peer := (leader + i) % len(ck.servers)
			reply := new(JoinReply)
			ok := ck.servers[peer].Call("ShardCtrler.Join", args, reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := new(LeaveArgs)
	args.Gids, args.Cid, args.Seq = gids, ck.cid, ck.seq
	ck.seq++

	leader := int(atomic.LoadInt32(&ck.leader))
	for {
		for i := 0; i < len(ck.servers); i++ {
			peer := (leader + i) % len(ck.servers)
			reply := new(LeaveReply)
			ok := ck.servers[peer].Call("ShardCtrler.Leave", args, reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := new(MoveArgs)
	args.Shard, args.Gid, args.Cid, args.Seq = shard, gid, ck.cid, ck.seq
	ck.seq++

	leader := int(atomic.LoadInt32(&ck.leader))
	for {
		for i := 0; i < len(ck.servers); i++ {
			peer := (leader + i) % len(ck.servers)
			reply := new(MoveReply)
			ok := ck.servers[peer].Call("ShardCtrler.Move", args, reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
