package raft

import (
	"log"
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
	ELECTION_TIMEOUT_MIN = 300
	ELECTION_TIMEOUT_MAX = 600
)

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		log.Printf("[%v]: Term -> %v, state -> FOLLOWER\n", rf.me, args.Term)
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// todo(3B): candidate last log term is at least up to date as receiver's log, grant vote
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 任期小于本节点的不接受
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// 更新 term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		// term 改变重置 votedFor
		rf.votedFor = -1
	}

	log.Printf("[%v]: received heartbeat from %v\n", rf.me, args.LeaderId)
	rf.lastHeartBeat = time.Now()
	rf.resetElectionTimeout()
	// CANDIDATE 或者 FOLLOWER 收到比自己 term 高或相同 term 的 heartbeat
	// 退化为 FOLLOWER
	if rf.state != FOLLOWER {
		rf.moveStateTo(FOLLOWER)
	}

	if len(args.Entries) == 0 {
		// heartbeat
		reply.Success = true
		return
	}

	// todo(3B)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 调用该方法前需要获取锁，以确保锁释放后其他线程能看见修改
func (rf *Raft) moveStateTo(state State) {
	names := map[State]string{
		FOLLOWER:  "FOLLOWER",
		CANDIDATE: "CANDIDATE",
		LEADER:    "LEADER",
	}
	prevState := rf.state
	// 只有 candidate 是可以重入的，其他不需要重新设置状态
	if prevState != state || prevState == CANDIDATE {
		switch state {
		case FOLLOWER:
			rf.state = FOLLOWER
			rf.resetElectionTimeout()
		case CANDIDATE:
			rf.state = CANDIDATE
			rf.resetElectionTimeout()
			rf.votedFor = rf.me
			rf.currentTerm++
		case LEADER:
			rf.state = LEADER
			// volatile leader state reinitialized after election
			rf.nextIndex = make([]int, 0)
			rf.matchIndex = make([]int, 0)
		}
	}

	if prevState == CANDIDATE && state == CANDIDATE {
		log.Printf("[%v][Term %v]: candidate election timeout, reelection.\n", rf.me, rf.currentTerm)
	} else {
		log.Printf("[%v][Term %v]: from %v moving state to %v.\n", rf.me, rf.currentTerm, names[prevState], names[state])
	}
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = time.Duration(rand.Intn(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN)+ELECTION_TIMEOUT_MIN) * time.Millisecond
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.moveStateTo(CANDIDATE)

	currentTerm := rf.currentTerm
	me := rf.me

	votesCh := make(chan bool, len(rf.peers)-1)

	// 广播 RequestVote RPC
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			// request vote rpc 不需要重试
			reply := RequestVoteReply{}
			args := RequestVoteArgs{
				Term:        currentTerm,
				CandidateId: me,
				// todo(3B): log params
			}
			success := rf.sendRequestVote(idx, &args, &reply)
			if success {
				votesCh <- reply.VoteGranted
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					log.Printf("[%v]: Discovered higher term %v from peer %v, reverting to FOLLOWER.\n", me, reply.Term, idx)
					rf.currentTerm = reply.Term
					rf.moveStateTo(FOLLOWER)
					rf.votedFor = -1
				}
				rf.mu.Unlock()
			} else {
				votesCh <- false
				//log.Printf("[%v]: RequestVote RPC [%v->%v] failed.\n", rf.me, rf.me, idx)
			}
		}(i)
	}
	rf.mu.Unlock()

	voteGranted := 1
	votesReceived := 1
	electionTimeout := rf.electionTimeout

	for {
		select {
		case granted := <-votesCh:
			votesReceived++
			if granted {
				voteGranted++
			}
			if votesReceived == len(rf.peers) {
				goto EndVoteCollection
			}
		case <-time.After(electionTimeout):
			log.Printf("[%v]: Election vote collection timed out.\n", me)
			goto EndVoteCollection
		}
	}

EndVoteCollection:
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("[%v][Term %v]: granted %v votes.\n", rf.me, rf.currentTerm, voteGranted)

	// 获得的票数大于总节点数的一半则成为 leader
	// 反之则等待超时重新选举，或收到 leader 的 heartbeat 退回 follower
	if voteGranted > (len(rf.peers) / 2) {
		rf.moveStateTo(LEADER)
		// 立即广播 夜长梦多
		go rf.broadcastHeartbeat()
	} else if votesReceived != len(rf.peers) {
		// candidate 超时，重新选举
		go rf.startElection()
	} else {
		// 未获得足够票数，退回 FOLLOWER
		log.Printf("[%v]: Not enough votes, reverting to FOLLOWER for Term %v.\n", me, rf.currentTerm)
		rf.moveStateTo(FOLLOWER)
		rf.votedFor = -1          // 重置投票
		rf.resetElectionTimeout() // 重新开始选举计时
	}
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	currentTerm := rf.currentTerm

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			reply := AppendEntriesReply{}
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:     currentTerm,
				LeaderId: rf.me,
				Entries:  make([]Log, 0),
			}
			rf.mu.Unlock()
			success := rf.sendAppendEntries(idx, &args, &reply)
			if !success {
				//log.Printf("[%v]: AppendEntries RPC [%v->%v] failed.\n", rf.me, rf.me, idx)
			}
		}(i)
	}
	rf.mu.Unlock()

	// 需不需要堵塞到所有RPC goroutine结束?
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// 这里需要插一个 barrier 不然看不见最新的 lastHeartbeat
		rf.mu.Lock()

		switch rf.state {
		case FOLLOWER:
			// follower 超时转变为 candidate
			if time.Since(rf.lastHeartBeat) >= rf.electionTimeout {
				go rf.startElection()
			}
			break
		case CANDIDATE:
			// candidate 超时逻辑移动至 startElection
			break
		case LEADER:
			// 广播 heartbeat
			go rf.broadcastHeartbeat()
		}

		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// 10s 一次 heartbeat，得随机
		ms := 50 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
