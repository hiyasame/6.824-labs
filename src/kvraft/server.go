package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int64
	OpId    int
	OpType  string // "Get", "Put", "Append", "NoOp".
	Key     string
	Value   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db map[string]string

	waiters map[int]*sync.Cond
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{ClerkId: args.ClerkId, OpType: "Get", Key: args.Key}

	kv.rf.Start(op)

	if kv.waiters[op.OpId] != nil {
		log.Fatalf("waiter for OpId %d is exist", op.OpId)
	}

	kv.waiters[op.OpId] = sync.NewCond(&kv.mu)

	// 等待到 get 操作被同步到大部分 raft 节点上
	kv.waiters[op.OpId].Wait()

	kv.waiters[op.OpId] = nil

	reply.Value = kv.db[args.Key]
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{ClerkId: args.ClerkId, OpType: args.OpType, Key: args.Key, Value: args.Value}
	kv.rf.Start(op)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) executor() {
	for msg := range kv.applyCh {
		if kv.killed() {
			break
		}

		kv.mu.Lock()

		if msg.CommandValid {
			op := msg.Command.(Op)
			switch op.OpType {
			case "Put":
				kv.db[op.Key] = op.Value
				break
			case "Append":
				kv.db[op.Key] += op.Value
				break
			case "Get":
				// 通知 waiter
				cond := kv.waiters[op.OpId]
				if cond != nil {
					cond.Signal()
				}

				// do nothing
			default:
				log.Fatalf("Unknown operation type: %s", op.OpType)
			}
		}

		kv.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.waiters = make(map[int]*sync.Cond)

	go kv.executor()

	return kv
}
