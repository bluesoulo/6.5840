package shardkv

import (
	// "github.com/sasha-s/go-deadlock"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
	"strconv"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type OPType int
const (
	GET OPType = iota
	PUT 
	APPEND
	GC
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
	// mu deadlock.Mutex
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
	// client id 和 version 的映射
	clientMap map[int64]int
	// 跟踪客户端等待的请求结果
    waitCh map[int]chan Res

	persister *raft.Persister

	dead      int32
	//当前的config
	curConfig shardctrler.Config
	//分片的有效性
	shardValid map[int]bool
	//cfg num -> (shard -> db)
	toOutShards map[int]map[int]map[string]string
	//"shard->config number"
	comeInShards    map[int]int
	//cfg num -> (shard -> bool)
	garbageShards map[int]map[int]bool
}

func (kv *ShardKV) checkWroneShardLocked(key string) bool{
	shard := key2shard(key)
	return kv.curConfig.Num == 0 || kv.curConfig.Shards[shard] != kv.gid
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	// Your code here.
	// op := Op{OprateType:GET, Key:args.Key, ClientId: args.Id, Version: args.Version}
	op := Op{OprateType:GET, Key:args.Key, ClientId: nrand(), Version: 1}
	kv.mu.Lock()

	//检查到来的请求是否当前的group
	if kv.checkWroneShardLocked(args.Key) {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}


	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := make(chan Res, 1)
	kv.waitCh[index] = ch
	kv.mu.Unlock()

	timeout := time.After(1 * time.Second)
	select {
	case commitRes := <- ch:
		Log(dLeader, "S%d: -res3=%v", kv.me, commitRes)
		commitOp := commitRes.OP
		if commitRes.ERROR == OK || commitRes.ERROR == ErrRepeatRequest{
			
			if commitOp.ClientId == op.ClientId && commitOp.Version == op.Version {
				reply.Err = OK
				reply.Value = commitRes.Value
			} else {
				reply.Err = ErrWrongLeader
			}
		} else if commitRes.ERROR == ErrNoKey {
			reply.Err = ErrNoKey
			reply.Value = ""
		} else if commitRes.ERROR == ErrWrongGroup {
			reply.Err = ErrWrongGroup
		}
	case <- timeout:
		kv.mu.Lock()
		delete(kv.waitCh, index)
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
	}

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	// Your code here.
	op := Op{Key:args.Key,Value: args.Value, ClientId: args.Id, Version: args.Version}
	if args.Op == "Put" {
		op.OprateType = PUT
	} else {
		op.OprateType = APPEND
	}

	kv.mu.Lock()
	//检查到来的请求是否当前的group
	if kv.checkWroneShardLocked(args.Key) {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}

	if op.Version <= kv.clientMap[op.ClientId] {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	kv.mu.Unlock()

	//写日志不要占用锁
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	
	kv.mu.Lock()
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
		} else if commitRes.ERROR == ErrWrongGroup{
			reply.Err = ErrWrongGroup
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

func (kv *ShardKV) garbageCollect(op Op) Err{
	_, isleader := kv.rf.GetState()
	if !isleader {
		return ErrWrongLeader
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return ErrWrongLeader
	}

	kv.mu.Lock()
	ch := make(chan Res, 1)
	kv.waitCh[index] = ch
	kv.mu.Unlock()

	timeout := time.After(1 * time.Second)
	select {
	case commitRes := <- ch:
		commitOp := commitRes.OP
		if commitRes.ERROR == OK || commitRes.ERROR == ErrRepeatRequest {
			if commitOp.ClientId == op.ClientId && commitOp.Version == op.Version {
				return OK
			} else {
				return ErrWrongLeader
			}
		} else {
			return ErrWrongLeader
		}
	case <- timeout:
		kv.mu.Lock()
		delete(kv.waitCh, index)
		kv.mu.Unlock()
		return  ErrWrongLeader
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) updateConfigChange(cfg shardctrler.Config) {
	kv.mu.Lock()
    defer kv.mu.Unlock()
    if cfg.Num <= kv.curConfig.Num { //only consider newer config
        return
    }
    oldCfg := kv.curConfig
	toOutShard := kv.shardValid

    kv.shardValid, kv.curConfig = make(map[int]bool), cfg
    for shard, gid := range cfg.Shards {
        if gid != kv.gid {continue}
        if  toOutShard[shard] || oldCfg.Num == 0 {
            kv.shardValid[shard] = true
            delete(toOutShard, shard)
        } else {
            kv.comeInShards[shard] = oldCfg.Num
        }
    }
    if len(toOutShard) > 0 { // prepare data that needed migration
        kv.toOutShards[oldCfg.Num] = make(map[int]map[string]string)
        for shard := range toOutShard {
			readyKeys := make([]string, 0)
            outDb := make(map[string]string)
            for k, v := range kv.dataMap {
                if key2shard(k) == shard {
					readyKeys = append(readyKeys, k)
                    outDb[k] = v
                    delete(kv.dataMap, k)
					// Log(dSERVER, "S-%v-%d:delete old data, config.Num=%v, shard=%v,key=%v,value=%v", kv.gid,kv.me, oldCfg.Num, shard, k, v)
                }
            }
            kv.toOutShards[oldCfg.Num][shard] = outDb
			Log(dSERVER, "S-%v-%d: ready data, config.Num=%v, shard=%v,keys=%v", kv.gid,kv.me, oldCfg.Num, shard,readyKeys)
        }
    }
}


func (kv *ShardKV) updateMapAfterPullShard(reply PullShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if reply.ConfigNum != kv.curConfig.Num - 1 {
		return
	}
	
	delete(kv.comeInShards, reply.Shard)

	if _, ok := kv.shardValid[reply.Shard]; !ok{
		for k, v := range(reply.Data) {
			kv.dataMap[k] = v
		}
		for id, seq := range(reply.ClientMap) {
			kv.clientMap[id] = max(seq, kv.clientMap[id])
		}

		kv.shardValid[reply.Shard] = true

		if _, ok := kv.garbageShards[reply.ConfigNum]; !ok {
			kv.garbageShards[reply.ConfigNum] = make(map[int]bool)
		}
		kv.garbageShards[reply.ConfigNum][reply.Shard] = true
	}
	Log(dSERVER, "S-%v-%d:update data, config.Num=%v, shardValid=%v,shard=%v", kv.gid,kv.me, reply.ConfigNum, kv.shardValid,reply.Shard)
}

func (kv *ShardKV) getInfo2OutShards () map[int][]int{
	configNums := make(map[int][]int)
	for num,shards := range(kv.toOutShards) {
		configNums[num] = make([]int, 0)
		for shard := range(shards) {
			if len(kv.toOutShards[num][shard]) > 0 {
				configNums[num] = append(configNums[num], shard)
			}
		}
	}
	return configNums
}

func (kv *ShardKV) gcLocked(configNum int, shard int) {

	if _, ok := kv.toOutShards[configNum]; ok {
		delete(kv.toOutShards[configNum], shard)
		if len(kv.toOutShards[configNum]) == 0 {
			delete(kv.toOutShards, configNum)
		}
	}
	// outShardsInfo := kv.getInfo2OutShards()
	Log(dClient, "S-%v-%d: gc toOutShards configNum=%v, shard=%v", kv.gid,kv.me,configNum,shard,)
}

func (kv *ShardKV) performOpOnMachine(applyMsg raft.ApplyMsg) {
	op := applyMsg.Command.(Op)
	res := Res{OP: op, Value: ""}

	kv.mu.Lock()
	if op.OprateType == GC {
		cfgNum,_ := strconv.Atoi(op.Key)
		kv.gcLocked(cfgNum, op.Version)
	} else {
		if !kv.shardValid[key2shard(op.Key)] {
			res.ERROR = ErrWrongGroup
		} else {
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
						res.Value = value
					} else {
						res.ERROR = ErrNoKey
					}
				}
				Log(dSERVER, "S-%v-%d: op = %d, shard=%v,kv.dataMap[%v] = %v", kv.gid, kv.me, op.OprateType, key2shard(op.Key),op.Key, kv.dataMap[op.Key])
				//执行该操作之后更新对应的版本号
				kv.clientMap[op.ClientId] = op.Version				
			} else {
				res.ERROR = ErrRepeatRequest
			}
		}
	}
					
	//放入日志Index对应的channel中
	if ch, ok := kv.waitCh[applyMsg.CommandIndex]; ok {
		ch <- res
		delete(kv.waitCh, applyMsg.CommandIndex)
	}

	kv.mu.Unlock()
}

func (kv *ShardKV) needSnapShot() bool {
    kv.mu.Lock()
    defer kv.mu.Unlock()
    threshold := 10
    return kv.maxraftstate > 0 &&
        kv.maxraftstate - kv.persister.RaftStateSize() < kv.maxraftstate/threshold
}

func (kv *ShardKV) doSnapShot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()

	e.Encode(kv.dataMap)
	e.Encode(kv.clientMap)
	e.Encode(kv.toOutShards)
	e.Encode(kv.comeInShards)
	e.Encode(kv.shardValid)
	e.Encode(kv.garbageShards)
	e.Encode(kv.curConfig)
	kv.mu.Unlock()

	// Log(dInfo, "S-%v-%d: need do snapshot! index=%v, kv.RaftStateSize=%v", kv.gid, kv.me, index, kv.persister.RaftStateSize())
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *ShardKV) applier() {
	for kv.killed() == false {
		applyMsg := <- kv.applyCh
		if applyMsg.CommandValid {
			if cfg, ok := applyMsg.Command.(shardctrler.Config); ok {
				kv.updateConfigChange(cfg)
			} else if reply, ok := applyMsg.Command.(PullShardReply); ok {
				kv.updateMapAfterPullShard(reply)
			} else {
				kv.performOpOnMachine(applyMsg)
			}

			if kv.needSnapShot() {
				go kv.doSnapShot(applyMsg.CommandIndex)
			}
		} else if applyMsg.SnapshotValid {
			// Log(dInfo, "S-%v-%d: read snapshot! applyMsg.SnapshotIndex=%v", kv.gid, kv.me, applyMsg.SnapshotIndex)
			//处理Follower InstallSnapshot的情况
			kv.readSnapshotData(applyMsg.Snapshot)
		}
	}
}


func (kv *ShardKV) readSnapshotData(data[] byte) {
	
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var dataMap map[string]string
	var clientMap map[int64]int

	var shardValid map[int]bool
	var comeInShards map[int]int
	var toOutShards map[int]map[int]map[string]string
	var garbageShards map[int]map[int]bool
	curConfig := shardctrler.Config{}
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return 
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if d.Decode(&dataMap) != nil || 
	   d.Decode(&clientMap) != nil || 
	   d.Decode(&toOutShards) != nil ||
	   d.Decode(&comeInShards) != nil ||
	   d.Decode(&shardValid) != nil ||
	   d.Decode(&garbageShards) != nil ||
	   d.Decode(&curConfig) != nil {
		Log(dError, "readSnapShot ERROR for server %v", kv.me)
	} else {
		kv.dataMap = dataMap
		kv.clientMap = clientMap
		kv.toOutShards = toOutShards
		kv.comeInShards = comeInShards
		kv.shardValid = shardValid
		kv.garbageShards = garbageShards
		kv.curConfig = curConfig
	} 
}



func (kv *ShardKV) PullShardData(args *PullShardArgs, reply *PullShardReply) {
	
	if _, isleader := kv.rf.GetState(); !isleader{
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ConfigNum >= kv.curConfig.Num {
		reply.Err = ErrWrongGroup
		return
	}


	data := make(map[string]string)
	clientMap := make(map[int64]int)

	for k, v := range(kv.toOutShards[args.ConfigNum][args.Shard]) {
		data[k] = v
	}

	for k,v := range(kv.clientMap) {
		clientMap[k] = v
	}

	reply.ConfigNum = args.ConfigNum
	reply.Shard = args.Shard
	reply.Data = data
	reply.ClientMap = clientMap 
	reply.Err = OK

	Log(dSERVER, "S-%v-%d: pull shard data, oldNum=%v, shard=%v", kv.gid, kv.me, reply.ConfigNum, reply.Shard)
}

func (kv *ShardKV) GarbageCollect(args *GarbageCollectArgs, reply *GarbageCollectReply) {
	if _, isleader := kv.rf.GetState(); !isleader{
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if _, ok := kv.toOutShards[args.ConfigNum]; !ok {
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.toOutShards[args.ConfigNum][args.Shard]; !ok {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{GC,strconv.Itoa(args.ConfigNum),"",nrand(),args.Shard}

	reply.Err =  kv.garbageCollect(op)
}

func (kv *ShardKV) pollNewConfig() {
	for kv.killed() == false {
		_, isleader := kv.rf.GetState()
		
		kv.mu.Lock()
		if isleader && len(kv.comeInShards) == 0 {
			num := kv.curConfig.Num
			config := kv.mck.Query(num + 1)

			if config.Num == num + 1{
				Log(dSERVER, "S-%v-%d:pollNewConfig, num=%v", kv.gid, kv.me, num+1)
				kv.rf.Start(config)
			}
		} 
		kv.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) pollShardData() {
	for kv.killed() == false {
		_, isleader := kv.rf.GetState()
		kv.mu.Lock()	
		if isleader && len(kv.comeInShards) > 0 {
			var wg sync.WaitGroup
			for shard, num := range(kv.comeInShards) {
				wg.Add(1)
				conf := kv.mck.Query(num)
				servers := conf.Groups[conf.Shards[shard]]
				args := PullShardArgs{Shard: shard, ConfigNum: conf.Num}
				Log(dSERVER, "S-%v-%d:pull shard request, oldNum=%v, shard=%v", kv.gid, kv.me,num, shard)
				go func (){
					for _, server := range servers {
						reply := PullShardReply{}
						ck := kv.make_end(server)
						success := ck.Call("ShardKV.PullShardData", &args, &reply)
						if success && reply.Err == OK {
							kv.rf.Start(reply)
							break
						}
					}
					wg.Done()
				}()
			}
			//注意这里释放锁的时机
			kv.mu.Unlock()
			wg.Wait()
		} else {
			kv.mu.Unlock()
		}
		time.Sleep(35 * time.Millisecond)
	}
	
}

func (kv *ShardKV) garbageCollecter() {
	for kv.killed() == false {
		_, isleader := kv.rf.GetState()
		kv.mu.Lock()
		if isleader && len(kv.garbageShards) > 0 {
			var wg sync.WaitGroup
			for num, shards := range(kv.garbageShards) {
				conf := kv.mck.Query(num)
				for shard := range(shards) {
					wg.Add(1)
					args := GarbageCollectArgs{Shard: shard, ConfigNum: num}
					servers := conf.Groups[conf.Shards[shard]]
					// Log(dSERVER, "S-%v-%d:garbage collect request, oldNum=%v, shard=%v", kv.gid, kv.me,num, shard)
					go func () {
						for _, server := range servers {
							reply := GarbageCollectReply{}
							ck := kv.make_end(server)
							success := ck.Call("ShardKV.GarbageCollect", &args, &reply)
							if success && reply.Err == OK {
								//使用写入Raft， 然后再删除garbageShards？
								kv.mu.Lock()
								delete(kv.garbageShards[args.ConfigNum], args.Shard)
								if len (kv.garbageShards[args.ConfigNum]) == 0 {
									delete(kv.garbageShards, args.ConfigNum)
								}
								kv.mu.Unlock()
								break
							}
						}
						wg.Done()
					}()
				}
			}
			kv.mu.Unlock()
			wg.Wait()
		} else {
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
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
	labgob.Register(shardctrler.Config{})
	labgob.Register(PullShardArgs{})
	labgob.Register(PullShardReply{})
	labgob.Register(GarbageCollectArgs{})
	labgob.Register(GarbageCollectReply{})

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
	kv.waitCh = make(map[int]chan Res)

	kv.comeInShards = make(map[int]int)
	kv.toOutShards = make(map[int]map[int]map[string]string)
	kv.shardValid = make(map[int]bool)
	kv.garbageShards = make(map[int]map[int]bool)
	
	kv.persister = persister
	kv.readSnapshotData(persister.ReadSnapshot())

	go kv.applier()
	go kv.pollNewConfig()
	go kv.pollShardData()
	go kv.garbageCollecter()
	return kv
}
