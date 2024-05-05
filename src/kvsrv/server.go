package kvsrv

import (
	"log"
	"sync"
	"time"
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
	// 
	clientMap map[int64]int64
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	id := args.Id
	ver := args.Ver

	ver2, ok := kv.clientMap[id]
	if ok {

	} else {
		if ver2 == ver {
			
		}
	}
	
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	val := args.Value
	id := args.Id

	//每次访问记录访问的id，完成duplicate detection
	_, ok := kv.idTimeMap[id]

	if !ok {
		old := kv.dataMap[key]
		kv.dataMap[key] = val
		reply.Value = old
		kv.idTimeMap[id] = time.Now()
	}

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	val := args.Value
	id := args.Id

	old := kv.dataMap[key]
	_, ok := kv.idTimeMap[id]

	if !ok {
		kv.dataMap[key] = old + val
		reply.Value = old
		kv.idTimeMap[id] = time.Now()
	}
	
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.dataMap = make(map[string]string)
	kv.clientMap = make(map[int64]int64)

	// //定时清理过期的idTimeMap
	// go func ()  {
	// 	for {
	// 		time.Sleep(3 * time.Second)
	// 		kv.mu.Lock()
	// 		for id, value := range kv.idTimeMap {
	// 			if time.Since(value).Seconds() > 3 {
	// 				delete(kv.idTimeMap, id)
	// 			}
	// 		}
	// 		kv.mu.Unlock()
	// 	}	
	// }()
	return kv
}