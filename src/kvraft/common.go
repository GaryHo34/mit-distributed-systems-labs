package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type ClientInfo struct {
	ClientId int64
	SeqNum   int
}

type Err string

type GetArgs struct {
	Key string
	ClientInfo
}

type GetReply struct {
	Value string
	Err   Err
}

// Put or Append
type PutAppendArgs struct {
	OpStr string // "Put" or "Append"
	Key   string
	Value string
	ClientInfo
}

type PutAppendReply struct {
	Err Err
}

type RaftReply struct {
	ClientInfo
	GetReply
}
