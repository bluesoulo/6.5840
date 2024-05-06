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
	// kv
	dataMap map[string]string

	olDdataMap map[int64]string
	// client id 和 version 的映射
	clientMap map[int64]int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()

	reply.Value = kv.dataMap[args.Key]
	
	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()

	key := args.Key
	val := args.Value
	id := args.Id
	ver := args.Ver

	old := kv.dataMap[key]
	ver2 := kv.clientMap[id]
	if ver2 == ver {
		kv.dataMap[key] = val
		reply.Value = old
		kv.clientMap[id] = ver2 + 1
	}

	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()

	key := args.Key
	val := args.Value
	id := args.Id
	ver := args.Ver

	ver2 := kv.clientMap[id]
	old := kv.dataMap[key]
	reply.Ver = ver2
	if ver2 == ver {
		kv.olDdataMap[id] = old
		kv.dataMap[key] = old + val
		reply.Value = old
		kv.clientMap[id] = ver2 + 1
	} else {
		reply.Value = kv.olDdataMap[id]
	}

	kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.dataMap = make(map[string]string)
	kv.clientMap = make(map[int64]int64)
	kv.olDdataMap = make(map[int64]string)
	return kv
}
