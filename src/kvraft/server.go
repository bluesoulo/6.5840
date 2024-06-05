package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

type OPType int
const (
	GET OPType = iota
	PUT 
	APPEND
	NOOP
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OprateType OPType
	Key string
	Value string

	ClientId int64
	Version int
}

type Res struct {
	OP Op
	Value string
	ERROR string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// kv
	dataMap map[string]string
	//保存某一个client 最后一次get的结果
	olDdataMap map[int64]string
	// client id 和 version 的映射
	clientMap map[int64]int
	// 跟踪客户端等待的请求结果
    waitCh map[int]chan Res
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{OprateType:NOOP, Key:args.Key, ClientId: args.Id, Version: args.Ver}

	kv.mu.Lock() 

	if args.Ver <= kv.clientMap[args.Id] {
		Log(dInfo, "error: args.Ver=%v,kv.clientMap[%v]=%v, olDdataMap[%v]=%v", args.Ver, args.Id, kv.clientMap[args.Id],args.Id, kv.olDdataMap[args.Id])
		reply.Err = OK
		reply.Value = kv.olDdataMap[args.Id]
		kv.mu.Unlock()
		return
	}

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	ch := make(chan Res, 1)
	kv.waitCh[index] = ch
	kv.mu.Unlock()


	// 设置超时时间 1 秒
    timeout := time.After(1 * time.Second)

	select {
	case commitRes := <- ch:
		Log(dLeader, "S%d: -res3=%v", kv.me, commitRes)
		commitOp := commitRes.OP
		if commitRes.ERROR == OK || commitRes.ERROR == ErrRepeatRequest{
			
			if commitOp.ClientId == op.ClientId && commitOp.Version == op.Version {
				reply.Err = OK
				kv.mu.Lock()
				reply.Value = commitRes.Value
				kv.mu.Unlock()
			} else {
				// kv.mu.Lock()
				// delete(kv.waitCh, index)
				reply.Err = ErrWrongLeader
				// kv.mu.Unlock()
			}
		} else if commitRes.ERROR == ErrNoKey {
			reply.Err = ErrNoKey
			reply.Value = ""
		}
	case <- timeout:
		kv.mu.Lock()
		delete(kv.waitCh, index)
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) putAndAppend(op * Op, reply *PutAppendReply) {
	kv.mu.Lock()

	if op.Version <= kv.clientMap[op.ClientId] {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	
	index, _, isLeader := kv.rf.Start(*op)
	
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	
	ch := make(chan Res, 1)
	kv.waitCh[index] = ch
	// Log(dLeader, "S%v:kv.waitCh=%v", kv.me,kv.waitCh)
	kv.mu.Unlock()

	// 设置超时时间 1 秒
    timeout := time.After(1 * time.Second)
	select {
	case commitRes := <- ch:
		commitOp := commitRes.OP
		if commitRes.ERROR == OK || commitRes.ERROR == ErrRepeatRequest {
			if commitOp.ClientId == op.ClientId && commitOp.Version == op.Version {
				reply.Err = OK
			} else {
				reply.Err = ErrWrongLeader
			}
		} else {
			kv.mu.Lock()
			delete(kv.waitCh, index)
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
		}
	case <- timeout:
		kv.mu.Lock()
		delete(kv.waitCh, index)
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
	}

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{OprateType:PUT,
		  Key:args.Key,
		  Value: args.Value,
		  ClientId: args.Id,
		  Version: args.Ver,
		}
	kv.putAndAppend(&op, reply)
}


func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{OprateType:APPEND,
		Key:args.Key,
		Value: args.Value,
		ClientId: args.Id,
		Version: args.Ver,
	  }
	kv.putAndAppend(&op, reply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		applyMsg := <- kv.applyCh
		op := applyMsg.Command.(Op)
		
		if applyMsg.CommandValid {

			res := Res{OP: op, Value: ""}
			
			kv.mu.Lock()
			
			
			//屏蔽重复请求
			if kv.clientMap[op.ClientId] < op.Version {
				Log(dLeader, "S%d: commandIndex=%v,op=%v, applyMsg=%v,clientMap[%v]=%v", kv.me, applyMsg.CommandIndex,op, applyMsg,op.ClientId,kv.clientMap[op.ClientId])
				
				res.ERROR = OK
				if op.OprateType == PUT {
					kv.dataMap[op.Key] = op.Value
				} else if op.OprateType == APPEND {
					kv.dataMap[op.Key] = kv.dataMap[op.Key] + op.Value
				} else {
					value, exists := kv.dataMap[op.Key]
					if exists {
						kv.olDdataMap[op.ClientId] = value
						res.Value = value
					} else {
						kv.olDdataMap[op.ClientId] = ""
						res.ERROR = ErrNoKey
					}
				}
				_, isleader := kv.rf.GetState()
				if isleader {
					Log(dLeader, "S%d: datdmap=%v", kv.me, kv.dataMap)
					Log(dLeader, "S%d: -res=%v", kv.me, res)
				}

				// Log(dLeader, "S%d:kv.waitCh=%v", kv.me, kv.waitCh)

				//执行该操作之后更新对应的版本号
				kv.clientMap[op.ClientId] = kv.clientMap[op.ClientId] + 1
			} else {
				res.ERROR = ErrRepeatRequest
				if op.OprateType != PUT && op.OprateType != APPEND {
					res.Value = kv.olDdataMap[op.ClientId]
				}
			}

			//放入日志Index对应的channel中
			if ch, ok := kv.waitCh[applyMsg.CommandIndex]; ok {
				Log(dLeader, "S%d: -2res=%v", kv.me, res)
				ch <- res
				// Log(dSERVER, "S%d:delete kv.waitCh =%v", kv.me, kv.waitCh)
				delete(kv.waitCh, applyMsg.CommandIndex)
			}

			kv.mu.Unlock()
		}
	}
}


// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.dataMap = make(map[string]string)
	kv.clientMap = make(map[int64]int)
	kv.olDdataMap = make(map[int64]string)
	kv.waitCh = make(map[int]chan Res)

	go kv.applier()

	return kv
}


