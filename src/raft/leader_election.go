package raft

import (
	"math/rand"
	"time"
)

type State int

const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)

// 论文中提到 election timeout 在 150~300 ms
// 但由于测试的原因，调整到一个更大的数值，但要确保能在 5s 内选举出 leader
// electionTimeout >> heartbeatInterval
// 通常 electionTimeout >= 3 × heartbeatInterval
const (
	ElectionTimeoutMin = 300
	ElectionTimeoutMax = 600
)

func (rf *Raft) changeTerm(term int) {
	rf.DLog("change term %v -> %v\n", rf.currentTerm, term)
	rf.currentTerm = term
}

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.acquireLock()
	defer rf.releaseLock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.changeTerm(args.Term)
		rf.moveStateTo(FOLLOWER)
		rf.votedFor = -1

		rf.persist()
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// candidate last log term is at least up to date as receiver's log, grant vote
		if (rf.lastLogTerm() == args.LastLogTerm && rf.lastLogIndex() <= args.LastLogIndex) || rf.lastLogTerm() < args.LastLogTerm {
			rf.votedFor = args.CandidateId
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			return
		}
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	if ok := rf.peers[server].Call("Raft.RequestVote", args, &reply); ok {
		rf.handleRequestVoteReply(server, args, &reply)
	}
}

func (rf *Raft) handleRequestVoteReply(from int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.acquireLock()
	defer rf.releaseLock()

	// 检查消息是否过时
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.moveStateTo(FOLLOWER)
		rf.currentTerm = args.Term
		defer rf.persist()
	}
	if rf.state == CANDIDATE && rf.currentTerm == args.Term {
		if reply.VoteGranted {
			rf.DLog("granted vote from %v", from)
			rf.votedMe[from] = true
			if rf.quorumVoted() {
				rf.moveStateTo(LEADER)
			}
		}
	}
}

func (rf *Raft) quorumVoted() bool {
	votes := 1
	for i, votedMe := range rf.votedMe {
		if i != rf.me && votedMe {
			votes++
		}
	}
	return 2*votes > len(rf.peers)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type FailureReason int

const (
	None FailureReason = iota
	LowTerm
	LogNotMatch
)

type AppendEntriesReply struct {
	Term    int
	Success bool
	Reason  FailureReason

	ConflictIndex int
	ConflictTerm  int
	LastLogIndex  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.acquireLock()
	defer rf.releaseLock()

	// 任期小于本节点的不接受
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.Reason = LowTerm
		return
	}

	// 更新 term
	if args.Term > rf.currentTerm {
		rf.changeTerm(args.Term)
		// term 改变重置 votedFor
		rf.votedFor = -1

		rf.persist()
	}

	//rf.DLog("received heartbeat from %v\n", args.LeaderId)
	rf.moveStateTo(FOLLOWER)

	if args.PrevLogIndex < rf.snapshot.LastIncludedIndex {
		reply.Term, reply.Success = rf.currentTerm, false
		reply.Reason = LogNotMatch
		reply.ConflictIndex = rf.snapshot.LastIncludedIndex + 1
		return
	}

	if args.PrevLogIndex > rf.lastLogIndex() {
		rf.DLog("prevLogIndex(%v) > lastLogIndex(%v)\n", args.PrevLogIndex, rf.lastLogIndex())
		reply.Term, reply.Success = rf.currentTerm, false
		reply.Reason = LogNotMatch
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastLogIndex() + 1
		return
	}

	//DPrintf("prevLogIndex: %v\n", args.PrevLogIndex)
	// if an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it(§5.3)
	if !rf.logMatched(args.PrevLogIndex, args.PrevLogTerm) {
		rf.DLog("log not matched: follower: [index: %v][term: %v], "+
			"leader: [index: %v][term: %v]\n", rf.lastLogIndex(), rf.lastLogTerm(), args.PrevLogIndex, args.PrevLogTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.Reason = LogNotMatch
		reply.ConflictTerm = rf.getLogTerm(args.PrevLogIndex)

		// 找到任期的第一条日志
		conflictIndex := args.PrevLogIndex
		for conflictIndex > rf.commitIndex &&
			rf.getLogTerm(conflictIndex) == reply.ConflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex

		return
	}

	//if args.PrevLogIndex != 0 {
	//	DPrintf("[%v]: log matched [index: %v][term: %v], append entries", rf.me, args.PrevLogIndex, args.PrevLogTerm)
	//}

	reply.Success = true
	reply.Term = rf.currentTerm
	originLog := rf.log
	rf.log = rf.log[:rf.realLogIndex(args.PrevLogIndex+1)]
	// append any new entries not already in the log
	logs := make([]Log, len(args.Entries))
	copy(logs, args.Entries)
	rf.log = append(rf.log, logs...)

	// log 发生改变时打印 debug 日志并保存
	if len(originLog) != len(rf.log) || len(args.Entries) != 0 {
		rf.DLog("log changed(append entries): %v\n", rf.log)
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.lastLogIndex())
		rf.DLog("commit index from leader -> %v\n", rf.commitIndex)
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); ok {
		rf.handleAppendEntries(server, args, reply)
	}
}

func (rf *Raft) handleAppendEntries(from int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.acquireLock()
	if !reply.Success {
		if reply.Term > rf.currentTerm {
			rf.moveStateTo(FOLLOWER)
			rf.changeTerm(reply.Term)
			rf.votedFor = -1

			rf.persist()
		} else if reply.Reason == LogNotMatch {
			// fails because of log inconsistency: decrement nextIndex & retry
			if reply.ConflictTerm == -1 {
				// Follower日志太短，直接设置为其最后索引+1
				rf.nextIndex[from] = reply.ConflictIndex
			} else {
				// 在Leader日志中查找冲突任期
				conflictIndex := rf.findLastLogInTerm(reply.ConflictTerm)
				if conflictIndex != -1 {
					// Leader也有该任期，设置为该任期最后一个条目的下一个
					rf.nextIndex[from] = Min(conflictIndex+1, reply.ConflictIndex)
				} else {
					// Leader没有该任期，设置为Follower该任期的第一个条目
					rf.nextIndex[from] = reply.ConflictIndex
				}
			}
			rf.nextIndex[from] = Max(rf.nextIndex[from], 1)
			rf.DLog("heartbeat rejected by %v because of log inconsistency. nextIndex: %v\n", from, rf.nextIndex[from])
			// retry: 下次心跳自动重试
		}
	} else {
		// 成功，不带 log 的心跳不需要做任何事，但如果发送了 log 则需要更新 leader 记录的状态
		if len(args.Entries) > 0 {
			rf.matchIndex[from] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[from] = rf.matchIndex[from] + 1
			// advance commit index if possible
			rf.advanceCommitIdx()
		}
	}
	rf.releaseLock()
}

// 调用该方法前需要获取锁，以确保锁释放后其他线程能看见修改
func (rf *Raft) moveStateTo(state State) {
	names := map[State]string{
		FOLLOWER:  "FOLLOWER",
		CANDIDATE: "CANDIDATE",
		LEADER:    "LEADER",
	}
	prevState := rf.state
	// candidate 是可以重入的，follower 也可以（重置计时器）
	if prevState != state || prevState == CANDIDATE || prevState == FOLLOWER {
		switch state {
		case FOLLOWER:
			rf.state = FOLLOWER
			rf.resetElectionTimeout()
		case CANDIDATE:
			rf.state = CANDIDATE
			rf.resetElectionTimeout()
			rf.votedFor = rf.me
			rf.votedMe = make([]bool, len(rf.peers))
			rf.votedMe[rf.me] = true
			rf.changeTerm(rf.currentTerm + 1)
			rf.persist()
		case LEADER:
			rf.state = LEADER
			// volatile leader state reinitialized after election
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for peer := range rf.peers {
				rf.matchIndex[peer], rf.nextIndex[peer] = 0, rf.lastLogIndex()+1
			}
		}
	}

	if prevState == CANDIDATE && state == CANDIDATE {
		rf.DLog("candidate election timeout, reelection.\n")
	} else if prevState != state {
		rf.DLog("from %v moving state to %v.\n", names[prevState], names[state])
	}
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = time.Duration(rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin)+ElectionTimeoutMin) * time.Millisecond
	rf.lastHeartBeat = time.Now()
}

func (rf *Raft) broadcastHeartbeat() {
	currentTerm := rf.currentTerm

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		if rf.nextIndex[i] <= rf.snapshot.LastIncludedIndex {
			snap := rf.cloneSnapshot()
			args := InstallSnapshotArgs{
				Term:              currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: snap.LastIncludedIndex,
				LastIncludedTerm:  snap.LastIncludedTerm,
				Data:              snap.Data,
			}
			go rf.sendInstallSnapshot(i, &args)
			continue
		}
		reply := AppendEntriesReply{}
		prevLogIndex := rf.nextIndex[i] - 1
		prevLogTerm := rf.snapshot.LastIncludedTerm
		lastLogIndex := rf.lastLogIndex()
		if prevLogIndex > rf.snapshot.LastIncludedIndex {
			prevLogTerm = rf.log[rf.realLogIndex(prevLogIndex)].Term
		}
		args := AppendEntriesArgs{
			Term:         currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
		}
		// 有可以发送的 log 就把 log 带上
		if lastLogIndex >= rf.nextIndex[i] {
			args.Entries = rf.log[rf.realLogIndex(rf.nextIndex[i]):]
		} else {
			args.Entries = make([]Log, 0)
		}
		go rf.sendAppendEntries(i, &args, &reply)
	}
}

func (rf *Raft) broadcastRequestVote() {
	for i := range rf.peers {
		if i != rf.me {
			lastLogIndex := rf.lastLogIndex()
			lastLogTerm := rf.snapshot.LastIncludedTerm
			if lastLogIndex > rf.snapshot.LastIncludedIndex {
				lastLogTerm = rf.log[rf.realLogIndex(lastLogIndex)].Term
			}
			args := RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
				// log params
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			go rf.sendRequestVote(i, &args)
		}
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// 这里需要插一个 barrier 不然看不见最新的 lastHeartbeat
		rf.acquireLock()

		switch rf.state {
		case FOLLOWER:
			// follower 超时转变为 candidate
			fallthrough
		case CANDIDATE:
			// follower 超时转变为 candidate
			// candidate 超时也需要重新选举
			if time.Since(rf.lastHeartBeat) >= rf.electionTimeout {
				rf.moveStateTo(CANDIDATE)
				rf.broadcastRequestVote()
			}
			break
		case LEADER:
			// 广播 heartbeat
			rf.broadcastHeartbeat()
		}

		rf.releaseLock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// 50-100ms 一次 heartbeat，得随机
		ms := 20 + (rand.Int63() % 20)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
