package shardkv


import "6.5840/labrpc"
import "6.5840/raft"
import "sync"
import "6.5840/labgob"
import "time"
import "bytes"
import "6.5840/shardctrler"

type OPType int
const (
	GET OPType = iota
	PUT 
	APPEND
)

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


type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	//定义一个shardctrler的客户端
	mck *shardctrler.Clerk
	
	//kv map
	dataMap map[string]string
	//保存某一个client 最后一次get的结果
	olDdataMap map[int64]string
	// client id 和 version 的映射
	clientMap map[int64]int
	// 跟踪客户端等待的请求结果
    waitCh map[int]chan Res

	persister *raft.Persister
	//记录Raft中提交的日志，被应用到状态机的Log index
	lastAppliedIndex int
}

func (kv *ShardKV) checkWroneShardLocked(key string) bool{
	shard := key2shard(key)
	config := kv.mck.Query(-1)
	return config.Shards[shard] != kv.gid
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{OprateType:GET, Key:args.Key, ClientId: args.Id, Version: args.Version}
	kv.mu.Lock()

	//检查到来的请求是否当前的group
	if kv.checkWroneShardLocked(args.Key) {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}

	if args.Version <= kv.clientMap[args.Id] {
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

	timeout := time.After(1 * time.Second)
	select {
	case commitRes := <- ch:
		// Log(dLeader, "S%d: -res3=%v", kv.me, commitRes)
		commitOp := commitRes.OP
		if commitRes.ERROR == OK || commitRes.ERROR == ErrRepeatRequest{
			
			if commitOp.ClientId == op.ClientId && commitOp.Version == op.Version {
				reply.Err = OK
				kv.mu.Lock()
				reply.Value = commitRes.Value
				kv.mu.Unlock()
			} else {

				reply.Err = ErrWrongLeader
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Key:args.Key,Value: args.Value, ClientId: args.Id, Version: args.Version}
	kv.mu.Lock()

	//检查到来的请求是否当前的group
	if kv.checkWroneShardLocked(args.Key) {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}

	if args.Op == "Put" {
		op.OprateType = PUT
	} else {
		op.OprateType = APPEND
	}

	if op.Version <= kv.clientMap[op.ClientId] {
		kv.mu.Unlock()
		reply.Err = OK
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
			reply.Err = ErrWrongLeader
		}
	case <- timeout:
		kv.mu.Lock()
		delete(kv.waitCh, index)
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


func (kv *ShardKV) applier() {
	for {
		applyMsg := <- kv.applyCh
		
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)
			res := Res{OP: op, Value: ""}
			
			kv.mu.Lock()
			
			//屏蔽重复请求
			if kv.clientMap[op.ClientId] < op.Version {
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

				//执行该操作之后更新对应的版本号
				kv.clientMap[op.ClientId] = op.Version				
			} else {
				res.ERROR = ErrRepeatRequest
				if op.OprateType != PUT && op.OprateType != APPEND {
					res.Value = kv.olDdataMap[op.ClientId]
				}
			}
			//状态机记录最大被应用的logIndex
			kv.lastAppliedIndex = applyMsg.CommandIndex
			//放入日志Index对应的channel中
			// Log(dLeader, "S%d: -2res=%v, kv.waitCh[%d]=%v", kv.me, res,applyMsg.CommandIndex, kv.waitCh[applyMsg.CommandIndex])
			if ch, ok := kv.waitCh[applyMsg.CommandIndex]; ok {
				ch <- res
				// Log(dSERVER, "S%d:delete kv.waitCh =%v", kv.me, kv.waitCh)
				delete(kv.waitCh, applyMsg.CommandIndex)
			}

			kv.mu.Unlock()
		} else if applyMsg.SnapshotValid {
			//处理Follower InstallSnapshot的情况
			dataMap, clientMap, olDdataMap := kv.readSnapshotData(applyMsg.Snapshot)
			
			kv.mu.Lock()
			if dataMap != nil {				
				kv.dataMap = dataMap
				
			}
			if clientMap != nil {
				kv.clientMap = clientMap
			}
			if olDdataMap != nil {
				kv.olDdataMap = olDdataMap
			}

			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) readSnapshotData(data[] byte) (map[string]string, map[int64]int, map[int64]string) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return nil, nil, nil
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var dataMap map[string]string
	var clientMap map[int64]int
	var olDdataMap map[int64]string

	if d.Decode(&dataMap) != nil || d.Decode(&clientMap) != nil || d.Decode(&olDdataMap) != nil{
		return nil, nil, nil
	} else {
		return dataMap, clientMap, olDdataMap
	}
}


func (kv *ShardKV) readPersist(data []byte) ([]raft.LogItem, int){
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return nil, -1
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []raft.LogItem
	var lastIncludedIndex int
	var lastIncludedTerm int
	
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&lastIncludedIndex) != nil ||
	   d.Decode(&lastIncludedTerm) != nil ||
	   d.Decode(&log) != nil {
		return nil, -1
	} else {
		return log, lastIncludedIndex
	}
}


func (kv *ShardKV) detecter(){
	for  {
		if kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= kv.maxraftstate {
			raftState := kv.persister.ReadRaftState()
			log, lastIncludedIndex := kv.readPersist(raftState)

			dataMap, clientMap, olDdataMap := kv.readSnapshotData(kv.persister.ReadSnapshot())

			if dataMap == nil {
				dataMap = make(map[string]string)
			}
			if clientMap == nil {
				clientMap = make(map[int64]int)
			}
			if olDdataMap == nil {
				olDdataMap = make(map[int64]string)
			}

			kv.mu.Lock()
			lastAppliedIndex := kv.lastAppliedIndex 
			kv.mu.Unlock()


			logIndex := lastAppliedIndex - lastIncludedIndex
			//读取lastAppliedIndex需要上锁，可能在等待上锁的过程中lastAppliedIndex增大，使得出现了log不包含的元素
			if logIndex >= len(log) {
				logIndex = len(log) - 1
			}

			for i := 1; i <= logIndex; i++ {

				op := log[i].Command.(Op)
				
				if clientMap[op.ClientId] < op.Version {
					clientMap[op.ClientId] = op.Version
					if op.OprateType == PUT {
						dataMap[op.Key] = op.Value
					} else if op.OprateType == APPEND {
						dataMap[op.Key] = dataMap[op.Key] + op.Value
					} else {
						olDdataMap[op.ClientId] = dataMap[op.Key]
					}
				}

			}
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(dataMap)
			e.Encode(clientMap)
			e.Encode(olDdataMap)

			//这里不能直接使用lastAppliedIndex，因为我们最终并不是切割了lastAppliedIndex - lastIncludedIndex
			kv.rf.Snapshot(lastIncludedIndex + logIndex, w.Bytes())
		}
		time.Sleep(200 * time.Millisecond)
	}
}


// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	//创建shardctrler的客户端
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.dataMap = make(map[string]string)
	kv.clientMap = make(map[int64]int)
	kv.olDdataMap = make(map[int64]string)
	kv.waitCh = make(map[int]chan Res)

	kv.persister = persister
	kv.lastAppliedIndex = 0

	dataMap, clientMap, olDdataMap := kv.readSnapshotData(persister.ReadSnapshot())
	if dataMap != nil {
		kv.dataMap = dataMap
	}
	if clientMap != nil {
		kv.clientMap = clientMap
	}
	if olDdataMap != nil {
		kv.olDdataMap = olDdataMap
	}

	go kv.applier()
	go kv.detecter()

	return kv
}
