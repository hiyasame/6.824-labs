package shardctrler

import (
	"bytes"
	"fmt"
	"log"
	"sort"
	"time"

	"6.5840/raft"
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
	waiters          map[string]chan Op
	persister        *raft.Persister
	maxraftstate     int // snapshot if log grows this big
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

func (sc *ShardCtrler) rebalanceShard(config *Config) {
	if len(config.Groups) == 0 {
		config.Shards = [NShards]int{}
		return
	}

	gidToShards := make(map[int][]int)
	for gid := range config.Groups {
		gidToShards[gid] = []int{}
	}
	var orphanShards []int
	for shard, gid := range config.Shards {
		if _, ok := gidToShards[gid]; ok {
			gidToShards[gid] = append(gidToShards[gid], shard)
		} else {
			orphanShards = append(orphanShards, shard)
		}
	}

	gids := make([]int, 0, len(config.Groups))
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	for _, shard := range orphanShards {
		leastLoadedGid := gids[0]
		for _, gid := range gids {
			if len(gidToShards[gid]) < len(gidToShards[leastLoadedGid]) {
				leastLoadedGid = gid
			}
		}
		gidToShards[leastLoadedGid] = append(gidToShards[leastLoadedGid], shard)
	}

	for {
		mostLoadedGid, leastLoadedGid := gids[0], gids[0]
		for _, gid := range gids {
			if len(gidToShards[gid]) > len(gidToShards[mostLoadedGid]) {
				mostLoadedGid = gid
			}
			if len(gidToShards[gid]) < len(gidToShards[leastLoadedGid]) {
				leastLoadedGid = gid
			}
		}

		if len(gidToShards[mostLoadedGid])-len(gidToShards[leastLoadedGid]) <= 1 {
			break
		}

		// Move one shard from the end of the most loaded group's slice
		shardToMove := gidToShards[mostLoadedGid][len(gidToShards[mostLoadedGid])-1]
		gidToShards[mostLoadedGid] = gidToShards[mostLoadedGid][:len(gidToShards[mostLoadedGid])-1]
		gidToShards[leastLoadedGid] = append(gidToShards[leastLoadedGid], shardToMove)
	}

	var newShards [NShards]int
	for gid, shards := range gidToShards {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	config.Shards = newShards
}

func (sc *ShardCtrler) JoinHandler(op *Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	latestConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    len(sc.configs),
		Shards: latestConfig.Shards,
		Groups: make(map[int][]string),
	}
	for gid, servers := range latestConfig.Groups {
		newConfig.Groups[gid] = servers
	}

	for gid, servers := range op.Servers {
		newConfig.Groups[gid] = servers
	}

	sc.rebalanceShard(&newConfig)

	sc.configs = append(sc.configs, newConfig)
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

	// 使用复合 key 避免冲突
	waitKey := fmt.Sprintf("%d-%d", args.ClerkId, args.OpId)
	sc.mu.Lock()

	if _, ok := sc.waiters[waitKey]; !ok {
		sc.waiters[waitKey] = make(chan Op, 1)
	}
	ch := sc.waiters[waitKey]
	sc.mu.Unlock()

	select {
	case appliedOp := <-ch:
		if appliedOp.ClerkId == op.ClerkId && appliedOp.OpId == op.OpId {
			sc.mu.Lock()
			reply.Err = OK
			sc.mu.Unlock()
		} else {
			reply.Err = ErrGid
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}

	sc.mu.Lock()
	delete(sc.waiters, waitKey)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) LeaveHandler(op *Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	latestConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    len(sc.configs),
		Shards: latestConfig.Shards,
		Groups: make(map[int][]string),
	}

	for gid, servers := range latestConfig.Groups {
		newConfig.Groups[gid] = servers
	}

	for _, gid := range op.GIDs {
		delete(newConfig.Groups, gid)
	}

	sc.rebalanceShard(&newConfig)

	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		OpCode:  OpLeave,
		GIDs:    args.GIDs,
		ClerkId: args.ClerkId,
		OpId:    args.OpId,
	}
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	// 使用复合 key 避免冲突
	waitKey := fmt.Sprintf("%d-%d", args.ClerkId, args.OpId)
	sc.mu.Lock()

	if _, ok := sc.waiters[waitKey]; !ok {
		sc.waiters[waitKey] = make(chan Op, 1)
	}
	ch := sc.waiters[waitKey]
	sc.mu.Unlock()

	select {
	case appliedOp := <-ch:
		if appliedOp.ClerkId == op.ClerkId && appliedOp.OpId == op.OpId {
			reply.Err = OK
		} else {
			reply.Err = ErrGid
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}

	sc.mu.Lock()
	delete(sc.waiters, waitKey)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) MoveHandler(op *Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	oldConfigNum := len(sc.configs) - 1
	oldConfig := sc.configs[oldConfigNum]
	newConfig := Config{
		Num:    oldConfigNum + 1,
		Groups: make(map[int][]string),
		Shards: oldConfig.Shards,
	}

	for gid, servers := range oldConfig.Groups {
		newConfig.Groups[gid] = servers
	}

	newConfig.Shards[op.Shard] = op.GID
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		OpCode:  OpMove,
		Shard:   args.Shard,
		GID:     args.GID,
		ClerkId: args.ClerkId,
		OpId:    args.OpId,
	}
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	// 使用复合 key 避免冲突
	waitKey := fmt.Sprintf("%d-%d", args.ClerkId, args.OpId)
	sc.mu.Lock()

	if _, ok := sc.waiters[waitKey]; !ok {
		sc.waiters[waitKey] = make(chan Op, 1)
	}
	ch := sc.waiters[waitKey]
	sc.mu.Unlock()

	select {
	case appliedOp := <-ch:
		if appliedOp.ClerkId == op.ClerkId && appliedOp.OpId == op.OpId {
			reply.Err = OK
		} else {
			reply.Err = ErrGid
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}

	sc.mu.Lock()
	delete(sc.waiters, waitKey)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.DLog("Query called with Num=%d, ClerkId=%d, OpId=%d", args.Num, args.ClerkId, args.OpId)
	op := Op{
		OpCode:  OpQuery,
		Num:     args.Num,
		ClerkId: args.ClerkId,
		OpId:    args.OpId,
	}
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		sc.DLog("Query: not leader")
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	// 使用复合 key 避免冲突
	waitKey := fmt.Sprintf("%d-%d", args.ClerkId, args.OpId)
	sc.mu.Lock()

	if _, ok := sc.waiters[waitKey]; !ok {
		sc.waiters[waitKey] = make(chan Op, 1)
	}
	ch := sc.waiters[waitKey]
	sc.mu.Unlock()

	select {
	case appliedOp := <-ch:
		if appliedOp.ClerkId == op.ClerkId && appliedOp.OpId == op.OpId {
			sc.mu.Lock()
			sc.DLog("Query: appliedOp.Num=%d, len(configs)=%d", appliedOp.Num, len(sc.configs))
			if appliedOp.Num == -1 || appliedOp.Num >= len(sc.configs) {
				reply.Config = sc.configs[len(sc.configs)-1]
				sc.DLog("Query: returning latest config (Num=%d)", reply.Config.Num)
			} else {
				reply.Config = sc.configs[appliedOp.Num]
				sc.DLog("Query: returning config at index %d (Num=%d)", appliedOp.Num, reply.Config.Num)
			}
			reply.Err = OK
			sc.mu.Unlock()
		} else {
			sc.DLog("Query: operation mismatch, expected ClerkId=%d OpId=%d, got ClerkId=%d OpId=%d",
				op.ClerkId, op.OpId, appliedOp.ClerkId, appliedOp.OpId)
			reply.Err = ErrGid
		}
	case <-time.After(500 * time.Millisecond):
		sc.DLog("Query: timeout")
		reply.Err = ErrTimeout
	}

	sc.mu.Lock()
	delete(sc.waiters, waitKey)
	sc.mu.Unlock()
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
	sc.waiters = make(map[string]chan Op)
	sc.maxraftstate = 1000
	sc.persister = persister

	// Restore state from snapshot if available
	sc.readSnapshot(persister.ReadSnapshot())

	go sc.applier()

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
					sc.DLog("Applier: executing Join operation")
					sc.JoinHandler(&op)
					sc.DLog("Applier: after Join, configs length=%d, latest config Num=%d", len(sc.configs), sc.configs[len(sc.configs)-1].Num)
					break
				case OpLeave:
					sc.DLog("Applier: executing Leave operation")
					sc.LeaveHandler(&op)
					sc.DLog("Applier: after Leave, configs length=%d, latest config Num=%d", len(sc.configs), sc.configs[len(sc.configs)-1].Num)
					break
				case OpMove:
					sc.DLog("Applier: executing Move operation")
					sc.MoveHandler(&op)
					sc.DLog("Applier: after Move, configs length=%d, latest config Num=%d", len(sc.configs), sc.configs[len(sc.configs)-1].Num)
					break
				case OpQuery:
					sc.DLog("Applier: executing Query operation (nop)")
					// nop
					break
				default:
					log.Fatalf("Unknown operation type: %s", op.OpCode)
				}

				// 通知 waiter
				waitKey := fmt.Sprintf("%d-%d", op.ClerkId, op.OpId)

				sc.mu.Lock()
				sc.clerkLatestOpIds[op.ClerkId] = op.OpId
				ch, ok := sc.waiters[waitKey]
				sc.mu.Unlock()

				if ok {
					select {
					case <-ch: // drain the channel
					default:
					}
					ch <- op
				}

				if sc.persister.RaftStateSize() > sc.maxraftstate {
					sc.checkpoint(msg.CommandIndex)
				}
			}

			if msg.SnapshotValid {
				sc.mu.Lock()
				r := bytes.NewBuffer(msg.Snapshot)
				d := labgob.NewDecoder(r)

				if d.Decode(&sc.configs) != nil || d.Decode(&sc.clerkLatestOpIds) != nil {
					log.Fatalf("Invalid snapshot: %s", msg.Snapshot)
				}
				sc.mu.Unlock()
			}
		}
	}
}

func (sc *ShardCtrler) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(sc.configs) != nil || e.Encode(sc.clerkLatestOpIds) != nil {
		panic("failed to encode some fields")
	}
	return w.Bytes()
}

func (sc *ShardCtrler) checkpoint(index int) {
	snapshot := sc.makeSnapshot()
	sc.rf.Snapshot(index, snapshot)
}

func (sc *ShardCtrler) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var configs []Config
	var lastApplied map[int64]int
	if d.Decode(&configs) != nil || d.Decode(&lastApplied) != nil {
		log.Fatalf("failed to decode snapshot")
	} else {
		sc.configs = configs
		sc.clerkLatestOpIds = lastApplied
	}
}
