package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
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

	// 使用 (ClerkId, OpId) 作为 key 来避免冲突
	waiters map[string]*sync.Cond
	// 记录已经处理过的操作，避免重复执行
	lastApplied map[int64]int // ClerkId -> last applied OpId
	// 记录当前的 term，用于检测 leader 变更
	currentTerm int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 检查是否是重复操作
	if lastOpId, exists := kv.lastApplied[args.ClerkId]; exists && args.OpId <= lastOpId {
		reply.Value = kv.db[args.Key]
		reply.Err = OK
		return
	}

	op := Op{ClerkId: args.ClerkId, OpType: "Get", Key: args.Key, OpId: args.OpId}

	_, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 使用复合 key 避免冲突
	waitKey := fmt.Sprintf("%d-%d", args.ClerkId, args.OpId)
	if kv.waiters[waitKey] != nil {
		log.Fatalf("waiter for key %s already exists", waitKey)
	}

	kv.waiters[waitKey] = sync.NewCond(&kv.mu)

	// 等待到 get 操作被同步到大部分 raft 节点上
	kv.waiters[waitKey].Wait()

	delete(kv.waiters, waitKey)

	reply.Value = kv.db[args.Key]
	reply.Err = OK
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 检查是否是重复操作
	if lastOpId, exists := kv.lastApplied[args.ClerkId]; exists && args.OpId <= lastOpId {
		reply.Err = OK
		return
	}

	op := Op{ClerkId: args.ClerkId, OpType: args.OpType, Key: args.Key, Value: args.Value, OpId: args.OpId}
	_, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 使用复合 key 避免冲突
	waitKey := fmt.Sprintf("%d-%d", args.ClerkId, args.OpId)
	if kv.waiters[waitKey] != nil {
		log.Fatalf("waiter for key %s already exists", waitKey)
	}

	kv.waiters[waitKey] = sync.NewCond(&kv.mu)

	// 等待 op 被同步到大部分 raft 节点上
	kv.waiters[waitKey].Wait()

	delete(kv.waiters, waitKey)

	// 检查当前的 term 是否发生了变化，如果变化了说明可能发生了 leader 变更
	currentTerm, stillLeader := kv.rf.GetState()
	if !stillLeader || currentTerm != term {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK
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

		// 检查 term 是否发生了变化
		currentTerm, _ := kv.rf.GetState()
		if currentTerm > kv.currentTerm {
			// Term 发生了变化，清理所有等待的操作
			for _, cond := range kv.waiters {
				cond.Signal()
			}
			kv.waiters = make(map[string]*sync.Cond)
			kv.currentTerm = currentTerm
		}

		if msg.CommandValid {
			op := msg.Command.(Op)

			// 检查是否是重复操作
			if lastOpId, exists := kv.lastApplied[op.ClerkId]; exists && op.OpId <= lastOpId {
				// 重复操作，直接通知等待者但不执行操作
				waitKey := fmt.Sprintf("%d-%d", op.ClerkId, op.OpId)
				if cond := kv.waiters[waitKey]; cond != nil {
					cond.Signal()
				}
				kv.mu.Unlock()
				continue
			}

			// 执行操作
			switch op.OpType {
			case "Put":
				kv.db[op.Key] = op.Value
			case "Append":
				kv.db[op.Key] += op.Value
			case "Get":
				// do nothing
			default:
				log.Fatalf("Unknown operation type: %s", op.OpType)
			}

			// 更新已应用的操作ID
			kv.lastApplied[op.ClerkId] = op.OpId

			// 通知 waiter
			waitKey := fmt.Sprintf("%d-%d", op.ClerkId, op.OpId)
			if cond := kv.waiters[waitKey]; cond != nil {
				cond.Signal()
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
	kv.waiters = make(map[string]*sync.Cond)
	kv.lastApplied = make(map[int64]int)

	go kv.executor()

	return kv
}
