package raft

import (
	"sort"
)

type Log struct {
	Term    int
	Index   int
	Command interface{}
}

func (rf *Raft) lastLogIndex() int {
	// 0 是 dummy index，代表没有 log
	// 从 1 开始记
	if len(rf.log) == 0 {
		return rf.snapshot.LastIncludedIndex
	}
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.snapshot.LastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) realLogIndex(index int) int {

	if index < rf.snapshot.LastIncludedIndex {
		return -1
	}
	return index - rf.snapshot.LastIncludedIndex - 1
}

func (rf *Raft) getLogTerm(index int) int {
	if index <= 0 {
		return rf.snapshot.LastIncludedTerm
	}
	return rf.log[rf.realLogIndex(index)].Term
}

func (rf *Raft) findLastLogInTerm(term int) int {
	for i := rf.lastLogIndex(); i > 0; i-- {
		if rf.getLogTerm(i) == term {
			return i
		}
		if rf.getLogTerm(i) < term {
			break
		}
	}
	return -1
}

func (rf *Raft) findLogPositionByIndex(index int) int {
	for i := 0; i < len(rf.log); i++ {
		if rf.log[i].Index == index {
			return i
		}
		if i+1 < len(rf.log) && rf.log[i].Index < index && rf.log[i+1].Index > index {
			return i
		}
	}
	return len(rf.log) - 1
}

func (rf *Raft) compactToSnapshot() {
	if rf.snapshot.LastIncludedIndex == 0 {
		return
	}
	idx := rf.findLogPositionByIndex(rf.snapshot.LastIncludedIndex)
	rf.log = rf.log[idx+1:]
	rf.DLog("log changed(compaction)(snapshot index: %v): %v\n", rf.snapshot.LastIncludedIndex, rf.log)
	if rf.commitIndex < rf.snapshot.LastIncludedIndex {
		rf.commitIndex = rf.snapshot.LastIncludedIndex
	}
	if rf.lastApplied < rf.snapshot.LastIncludedIndex {
		rf.lastApplied = rf.snapshot.LastIncludedIndex
	}
	rf.applyCond.Signal()
}

func (rf *Raft) cloneSnapshot() Snapshot {
	// 论文中提到如果 snapshot 很大的话可以做 copy on write
	// 不过这里是简化实现，Data 很小
	snap := Snapshot{LastIncludedIndex: rf.snapshot.LastIncludedIndex, LastIncludedTerm: rf.snapshot.LastIncludedTerm, Data: make([]byte, len(rf.snapshot.Data))}
	copy(snap.Data, rf.snapshot.Data)
	return snap
}

func (rf *Raft) advanceCommitIdx() {
	n := len(rf.matchIndex)
	sortMatchIndex := make([]int, n)
	copy(sortMatchIndex, rf.matchIndex)
	sort.Ints(sortMatchIndex)
	// 获得中间的数，大部分节点的 match index 都比这个数大
	newCommitIndex := sortMatchIndex[n-(n/2+1)]
	// 如果可以更新，就更新
	if newCommitIndex > rf.commitIndex {
		rf.DLog("commitIndex -> %v\n", newCommitIndex)
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}
}

func (rf *Raft) logMatched(index int, term int) bool {
	return index <= rf.lastLogIndex() && (index <= rf.snapshot.LastIncludedIndex || term == rf.log[rf.realLogIndex(index)].Term)
}

func (rf *Raft) applier() {
	lastSnapshotIndex := 0
	for !rf.killed() {
		select {
		case snap := <-rf.snapshotCh:
			// 只 apply 更新的，忽略掉旧的
			if snap.LastIncludedIndex > lastSnapshotIndex {
				rf.acquireLock()
				rf.DLog("apply snapshot (index %v, term: %v, data: %v)\n", snap.LastIncludedIndex, snap.LastIncludedTerm, snap.Data)
				rf.releaseLock()
				rf.applyCh <- ApplyMsg{SnapshotValid: true, SnapshotIndex: snap.LastIncludedIndex, SnapshotTerm: snap.LastIncludedTerm, Snapshot: snap.Data}
				lastSnapshotIndex = snap.LastIncludedIndex
			}
		default:
		}
		rf.acquireLock()
		for rf.commitIndex > rf.lastApplied {
			// 这里是先 increment 再 apply
			rf.lastApplied++
			entry := rf.log[rf.realLogIndex(rf.lastApplied)]
			rf.DLog("apply log (index %v) in term %v\n", entry.Index, rf.currentTerm)

			rf.releaseLock()

			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}

			rf.acquireLock()

		}
		rf.applyCond.Wait()

		rf.releaseLock()
	}
}
