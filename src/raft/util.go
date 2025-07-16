package raft

import (
	"bytes"
	"fmt"
	"log"
	"runtime"
	"strconv"
)

// Debugging
const Debug = false
const LockTracing = false

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

// GetGID 从 Goroutine 的堆栈信息中提取其 ID。
// 这是一个非标准且依赖于运行时内部格式的方法，不应在生产代码中滥用。
func GetGID() uint64 {
	b := make([]byte, 64)           // 创建一个足够大的字节切片来存储堆栈信息
	b = b[:runtime.Stack(b, false)] // 获取当前 Goroutine 的堆栈信息

	// 堆栈信息通常以 "goroutine N [" 开头，N 就是 Goroutine ID
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	i := bytes.IndexByte(b, ' ')
	if i == -1 {
		panic(fmt.Sprintf("无法解析 Goroutine ID: %q", b))
	}
	// 将 ID 字符串转换为整数
	id, err := strconv.ParseUint(string(b[:i]), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("无法解析 Goroutine ID: %v", err))
	}
	return id
}

func (rf *Raft) acquireLock() {
	rf.mu.Lock()
	if LockTracing {
		rf.lockingGoroutineId = GetGID()
		//rf.DLog("goroutine %v acquire lock", rf.lockingGoroutineId)
	}
}

func (rf *Raft) releaseLock() {
	if LockTracing {
		//rf.DLog("goroutine %v release lock", rf.lockingGoroutineId)
		rf.lockingGoroutineId = 0
	}
	rf.mu.Unlock()
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
