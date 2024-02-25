package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	CId   int64
	Seq   int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	CId int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}
