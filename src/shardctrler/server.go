package shardctrler

import (
	"6.5840/raft"
	"log"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	lastAppliedIndex int
	clerkLatestOpIds map[int64]int
}

const (
	OpJoin  = "join"
	OpLeave = "leave"
	OpMove  = "move"
	OpQuery = "query"
)

type Op struct {
	// Your data here
	OpCode string
	// Join
	Servers map[int][]string // new GID -> servers mappings
	// Leave
	GIDs []int
	// Move
	Shard int
	GID   int
	// Query
	Num int // desired config number

	ClerkId int64
	OpId    int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		OpCode:  OpJoin,
		Servers: args.Servers,
		ClerkId: args.ClerkId,
		OpId:    args.OpId,
	}
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastAppliedIndex = 0
	sc.clerkLatestOpIds = make(map[int64]int)

	return sc
}

func (sc *ShardCtrler) applier() {
	for {
		select {
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				op, ok := msg.Command.(Op)
				if !ok {
					log.Fatalf("cast command to op failed")
				}
				sc.mu.Lock()

				// commandIndex 从 1 开始
				if msg.CommandIndex <= sc.lastAppliedIndex {
					sc.mu.Unlock()
					continue
				}

				sc.DLog("received applied message (%v)", msg)
				sc.lastAppliedIndex = msg.CommandIndex

				// 针对单个客户端是否重复
				if id, exists := sc.clerkLatestOpIds[op.ClerkId]; exists && id >= op.OpId {
					sc.mu.Unlock()
					continue
				}

				sc.mu.Unlock()

				switch op.OpCode {
				case OpJoin:
					// todo: handle join
					break
				case OpLeave:
					// todo handle leave
					break
				case OpMove:
					// todo handle move
					break
				case OpQuery:
					// todo handle query
					break
				}

				sc.mu.Lock()
				sc.clerkLatestOpIds[op.ClerkId] = op.OpId
				// todo 通知业务侧已完成
			}
		}
	}
}
