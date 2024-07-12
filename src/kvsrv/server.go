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

type LastMessage struct {
	argid uint
	value string
}

type KVServer struct {
	mu       sync.Mutex
	LastTask map[int]LastMessage
	kvcache  map[string]string
	// Your definitions here.
}

// ok means is same with the last task
func (kv *KVServer) checkDuplicate(argid uint, clientid int) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	Message, ok := kv.LastTask[clientid]
	if !ok { //第一次
		return "", false
	} else {
		if Message.argid == argid {
			return Message.value, true
		}
	}
	return "", false
}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	reply.Value = kv.kvcache[args.Key]
	kv.mu.Unlock()
	// Your code here.
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	_, ok := kv.checkDuplicate(args.ArgId, args.ClientId)
	if !ok {
		kv.mu.Lock()
		//reply.Value=kv.kvcache[args.Key]
		kv.kvcache[args.Key] = args.Value
		kv.LastTask[args.ClientId] = LastMessage{args.ArgId, ""}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	message, ok := kv.checkDuplicate(args.ArgId, args.ClientId)
	if ok {
		reply.Value = message
	} else {
		kv.mu.Lock()
		reply.Value = kv.kvcache[args.Key]
		kv.kvcache[args.Key] += args.Value
		kv.LastTask[args.ClientId] = LastMessage{args.ArgId, reply.Value}
		kv.mu.Unlock()
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvcache = make(map[string]string)
	kv.LastTask = make(map[int]LastMessage)
	// You may need initialization code here.

	return kv
}
