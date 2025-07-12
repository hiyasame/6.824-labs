package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = false

var names = map[State]string{
	FOLLOWER:  "FOLLOWER",
	CANDIDATE: "CANDIDATE",
	LEADER:    "LEADER",
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func (rf *Raft) DLog(format string, a ...interface{}) {
	newFormat := fmt.Sprintf("[%v][Term %v][%v]: %s", rf.me, rf.currentTerm, names[rf.state], format)
	DPrintf(newFormat, a...)
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
