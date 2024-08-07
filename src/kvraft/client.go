package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

var id = int64(0)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	clientId int64
	seqNum   int
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
	for {
		for _, server := range ck.servers {
			reply := GetReply{}
			ok := server.Call("KVServer.Get", &args, &reply)
			if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
				return reply.Value
			}
		}
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
	for {
		for _, server := range ck.servers {
			reply := PutAppendReply{}
			ok := server.Call("KVServer.PutAppend", &args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
