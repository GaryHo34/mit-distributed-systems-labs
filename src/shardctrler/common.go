package shardctrler

//
// Shard controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

// Copy the config from old to new
func (c *Config) Copy(cf *Config) {
	c.Num = cf.Num
	for i := 0; i < NShards; i++ {
		c.Shards[i] = cf.Shards[i]
	}
	c.Groups = make(map[int][]string)
	for k, v := range cf.Groups {
		c.Groups[k] = append([]string{}, v...)
	}
}

const (
	OK = "OK"
)

type Err string

type ClientInfo struct {
	Cid int64
	Seq int
}

type RaftReply struct {
	WrongLeader bool
	Err         Err
}

type JoinArgs struct {
	Srvs map[int][]string // new GID -> servers mappings
	ClientInfo
}

type LeaveArgs struct {
	Gids []int
	ClientInfo
}

type MoveArgs struct {
	Shard int
	Gid   int
	ClientInfo
}

type QueryArgs struct {
	ConfNum int // desired config number
	ClientInfo
}

type JoinReply = RaftReply

type LeaveReply = RaftReply

type MoveReply = RaftReply

type QueryReply struct {
	Conf *Config
	RaftReply
}

type Cache struct {
	Seq int
	QueryReply
}
