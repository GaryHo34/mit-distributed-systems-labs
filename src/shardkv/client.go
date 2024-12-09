package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	cid      int64
	seq      int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.cid = nrand()
	ck.seq = 1
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := new(GetArgs)
	args.Key = key
	args.Cid, args.Seq = ck.cid, ck.seq
	ck.seq++

	for {
		args.ConfNum = ck.config.Num
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				reply := new(GetReply)
				ok := srv.Call("ShardKV.Get", args, reply)
				if !ok { // Network error, try next server
					continue
				} else if reply.Err == OK || reply.Err == ErrNoKey { // Success
					return reply.Value
				} else if reply.Err == ErrWrongLeader {
					continue
				} else if reply.Err == ErrWrongGroup {
					break
				} else {
					panic("Get: unknown error")
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := new(PutAppendArgs)
	args.OpStr, args.Key, args.Value, args.Cid, args.Seq = op, key, value, ck.cid, ck.seq
	ck.seq++

	for {
		args.ConfNum = ck.config.Num
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				reply := new(PutAppendReply)
				ok := srv.Call("ShardKV.PutAppend", args, reply)
				if !ok { // Network error, try next server
					continue
				} else if reply.Err == OK { // Success
					return
				} else if reply.Err == ErrWrongLeader {
					continue
				} else if reply.Err == ErrWrongGroup {
					break
				} else {
					panic("PutAppend: unknown error")
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
