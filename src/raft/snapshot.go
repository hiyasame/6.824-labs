package raft

type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.acquireLock()
	defer rf.releaseLock()

	if index <= rf.snapshot.LastIncludedIndex {
		return
	}

	snapshotTerm := rf.getLogTerm(index)

	rf.snapshot.LastIncludedIndex = index
	rf.snapshot.LastIncludedTerm = snapshotTerm
	rf.snapshot.Data = snapshot

	rf.compactToSnapshot()

	rf.persist()
}

// lab中是简化实现，只有一个 data chunk
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	//Offset            int
	Data []byte
	//Done bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.acquireLock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.releaseLock()
		return
	}

	rf.moveStateTo(FOLLOWER)

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term

		rf.persist()
	}

	if args.LastIncludedIndex <= rf.commitIndex {
		rf.releaseLock()
		return
	}

	rf.snapshot.LastIncludedIndex = args.LastIncludedIndex
	rf.snapshot.LastIncludedTerm = args.LastIncludedTerm
	rf.snapshot.Data = args.Data

	rf.compactToSnapshot()

	rf.persist()

	snap := rf.cloneSnapshot()
	rf.releaseLock()
	go func() {
		rf.acquireLock()
		rf.applyCond.Signal()
		rf.releaseLock()
	}()
	rf.snapshotCh <- snap
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	if ok := rf.peers[server].Call("Raft.InstallSnapshot", args, &reply); ok {
		rf.handleInstallSnapshotReply(server, args, &reply)
	}
}

func (rf *Raft) handleInstallSnapshotReply(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.acquireLock()
	defer rf.releaseLock()

	if reply.Term > rf.currentTerm {
		rf.moveStateTo(FOLLOWER)
		rf.currentTerm = reply.Term

		rf.persist()
	}

	// 状态预检
	if rf.state != LEADER || rf.currentTerm != args.Term {
		return
	}

	// peer 之前的日志进度落后于 snapshot，更新为 snapshot index
	if rf.matchIndex[server] < args.LastIncludedIndex {
		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	}
}
