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
	dataMap map[string]string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) error{
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	val := kv.dataMap[key]
	reply.Value = val

	return nil
	// Your code here.
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) error{
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	val := args.Value
	old := kv.dataMap[key]
	kv.dataMap[key] = val
	reply.Value = old

	return nil
	// Your code here.
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) error{
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	val := args.Value
	old := kv.dataMap[key]
	kv.dataMap[key] = old + val
	reply.Value = old

	return nil
	// Your code here.
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.dataMap = make(map[string]string)
	return kv
}
