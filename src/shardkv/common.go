package shardkv

import "6.5840/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type ClientInfo struct {
	Cid     int64
	Seq     int
	ConfNum int
}

type RaftReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	ClientInfo
}

// Put or Append
type PutAppendArgs struct {
	OpStr string // "Put" or "Append"
	Key   string
	Value string
	ClientInfo
}

type ConfUpdateArgs struct {
	Conf *shardctrler.Config
	ClientInfo
}

type ShardData struct {
	Data    map[string]string
	Cache   map[int64]*Cache // cid -> Cache
	IsReady bool
}

type ShardPushTask struct {
	ConfNum   int
	Shard     int
	Gid       int
	Servers   []string
	ShardData ShardData
}

type ShardPushTaskArgs struct {
	Task *ShardPushTask
	ClientInfo
}

type ShardPushFinishArgs struct {
	Shard int
	Gid   int
	ClientInfo
}

func (t *ShardPushTaskArgs) Copy(from *ShardPushTaskArgs) {
	t.Cid = from.Cid
	t.Seq = from.Seq
	t.ConfNum = from.ConfNum
	t.Task = new(ShardPushTask)
	t.Task.ConfNum = from.Task.ConfNum
	t.Task.Shard = from.Task.Shard
	t.Task.Gid = from.Task.Gid
	t.Task.Servers = append([]string{}, from.Task.Servers...)
	t.Task.ShardData = ShardData{
		Data:    make(map[string]string),
		Cache:   make(map[int64]*Cache),
		IsReady: from.Task.ShardData.IsReady,
	}
	for k, v := range from.Task.ShardData.Data {
		t.Task.ShardData.Data[k] = v
	}
	for k, v := range from.Task.ShardData.Cache {
		t.Task.ShardData.Cache[k] = new(Cache)
		t.Task.ShardData.Cache[k].Copy(v)
	}
}

type GetReply struct {
	Value string
	RaftReply
}
type PutAppendReply = RaftReply

type ConfUpdateReply = RaftReply

type ShardPushTaskReply = RaftReply

type Cache struct {
	Seq int
	GetReply
}

func (c *Cache) Copy(from *Cache) {
	c.Seq = from.Seq
	c.Value = from.Value
	c.Err = from.Err
}
