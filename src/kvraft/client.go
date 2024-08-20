package kvraft

import (
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

var id = int64(0)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	clientId int64
	seqNum   int
	leader   int32 // cache the leader
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = id
	id++
	ck.seqNum = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.seqNum++
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqNum:   ck.seqNum,
	}
	leader := int(atomic.LoadInt32(&ck.leader))
	for {
		for i := 0; i < len(ck.servers); i++ {
			peer := (leader + i) % len(ck.servers)
			reply := GetReply{}
			ok := ck.servers[peer].Call("KVServer.Get", &args, &reply)
			if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
				atomic.StoreInt32(&ck.leader, int32(peer))
				return reply.Value
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.seqNum++
	args := PutAppendArgs{
		Op:       op,
		Key:      key,
		Value:    value,
		ClientId: ck.clientId,
		SeqNum:   ck.seqNum,
	}
	leader := int(atomic.LoadInt32(&ck.leader))
	for {
		for i := 0; i < len(ck.servers); i++ {
			peer := (leader + i) % len(ck.servers)
			reply := PutAppendReply{}
			ok := ck.servers[peer].Call("KVServer.PutAppend", &args, &reply)
			if ok && reply.Err == OK {
				atomic.StoreInt32(&ck.leader, int32(peer))
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
