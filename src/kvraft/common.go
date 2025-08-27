package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int64
	OpId    int
	OpType  string // "Put" or "Append"
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ClerkId int64
	OpId    int
	Key     string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
