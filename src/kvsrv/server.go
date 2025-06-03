package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvs     map[string]string
	history map[int64]*Request
}

type Request struct {
	LastSeq int64
	Value   string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.kvs[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if value, ok := kv.history[args.ClientID]; ok {
		if value.LastSeq >= args.Seq {
			// return value before append
			reply.Value = value.Value
			return
		}
	} else {
		kv.history[args.ClientID] = new(Request)
		kv.history[args.ClientID].LastSeq = -1
	}

	reply.Value = kv.kvs[args.Key]
	kv.history[args.ClientID].LastSeq = args.Seq
	kv.history[args.ClientID].Value = reply.Value
	kv.kvs[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, ok := kv.history[args.ClientID]; ok {
		if value.LastSeq >= args.Seq {
			// return value before append
			reply.Value = value.Value
			return
		}
	} else {
		kv.history[args.ClientID] = new(Request)
		kv.history[args.ClientID].LastSeq = -1
	}

	reply.Value = kv.kvs[args.Key]
	kv.history[args.ClientID].LastSeq = args.Seq
	kv.history[args.ClientID].Value = reply.Value
	kv.kvs[args.Key] += args.Value
}

func (kv *KVServer) Ack(args *AckArgs, reply *AckReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, ok := kv.history[args.ClientID]; ok {
		if value.LastSeq == args.Seq {
			delete(kv.history, args.ClientID)
		}
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.mu = sync.Mutex{}
	kv.kvs = make(map[string]string)
	kv.history = make(map[int64]*Request)

	return kv
}
