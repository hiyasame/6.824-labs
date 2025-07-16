package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// debug fields
	lockingGoroutineId uint64

	// states not mentioned in paper
	state           State
	electionTimeout time.Duration
	lastHeartBeat   time.Time
	applyCh         chan ApplyMsg
	votedMe         []bool
	snapshot        Snapshot
	snapshotCh      chan Snapshot

	// persist state on all
	currentTerm int
	votedFor    int
	log         []Log

	// volatile state on all
	commitIndex int
	lastApplied int

	// volatile state on leader
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.acquireLock()
	defer rf.releaseLock()

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	err = e.Encode(rf.votedFor)
	err = e.Encode(rf.log)
	err = e.Encode(rf.snapshot.LastIncludedIndex)
	err = e.Encode(rf.snapshot.LastIncludedTerm)
	if err != nil {
		panic(err)
	}
	rf.DLog("persist (currentTerm: %v, votedFor: %v, log: %v, snapshot: %v)\n", rf.currentTerm, rf.votedFor, rf.log, rf.snapshot)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot.Data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	rf.DLog("try read persist ...\n")
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Log
	snapshot := Snapshot{}
	err := d.Decode(&currentTerm)
	err = d.Decode(&votedFor)
	err = d.Decode(&log)
	err = d.Decode(&snapshot.LastIncludedIndex)
	err = d.Decode(&snapshot.LastIncludedTerm)
	snapshot.Data = rf.persister.ReadSnapshot()
	if err != nil {
		return
	}
	rf.DLog("read persist (currentTerm: %v, votedFor: %v, log: %v, snapshot: %v)\n", currentTerm, votedFor, log, snapshot)
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.snapshot = snapshot

	if rf.commitIndex < snapshot.LastIncludedIndex {
		rf.commitIndex = snapshot.LastIncludedIndex
	}
	if rf.lastApplied < snapshot.LastIncludedIndex {
		rf.lastApplied = snapshot.LastIncludedIndex
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.acquireLock()
	defer rf.releaseLock()

	index := rf.lastLogIndex() + 1
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	// Your code here (3B).
	if !isLeader {
		return -1, -1, false
	}

	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	rf.log = append(rf.log, Log{Command: command, Term: term, Index: index})

	rf.persist()

	// log 发生改变时打印
	rf.DLog("log changed(start): %v\n", rf.log)

	rf.DLog("starts argeement, command: %v, term: %v.\n", command, term)

	// 下次心跳会广播 rpc

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.mu = sync.Mutex{}
	rf.acquireLock()
	defer rf.releaseLock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.log = make([]Log, 0)
	rf.votedMe = make([]bool, len(rf.peers))
	rf.snapshot = Snapshot{
		LastIncludedIndex: 0,
		LastIncludedTerm:  0,
		Data:              make([]byte, 0),
	}
	rf.snapshotCh = make(chan Snapshot)

	// Your initialization code here (3A, 3B, 3C).
	rf.state = FOLLOWER
	rf.lastHeartBeat = time.Now()
	rf.resetElectionTimeout()

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for peer := range peers {
		rf.matchIndex[peer], rf.nextIndex[peer] = 0, rf.lastLogIndex()+1
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	// 启动 apply 线程
	go rf.applier()

	return rf
}
