package raft

import (
	"sort"
	"time"
)

type Log struct {
	Term    int
	Index   int
	Command interface{}
}

func (rf *Raft) firstLogIndex() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[0].Index
}

func (rf *Raft) lastLogIndex() int {
	// 0 是 dummy index，代表没有 log
	// 从 1 开始记
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLogTerm(index int) int {
	if index == 0 {
		return 0
	}
	return rf.log[index-1].Term
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
	}
}

func (rf *Raft) logMatched(index int, term int) bool {
	return index <= rf.lastLogIndex() && (index == 0 || term == rf.log[index-1].Term)
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			// 这里是先 increment 再 apply
			rf.lastApplied++
			entry := rf.log[rf.lastApplied-1]
			rf.DLog("apply log (index %v) in term %v\n", entry.Index, rf.currentTerm)

			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
}
