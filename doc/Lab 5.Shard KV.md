### 1.Shard KV介绍

​		分片控制器中维护了配置信息（分片，以及其隶属的分组的信息）。每一个分组组成的集群可以理解为一个KV Server，KV Server中保存了隶属于该分组所有分片的信息。

​		每个分片仅在该分片的Raft副本组中有大多数服务器存活并能够相互通信，并且能够与大多数shardctrler（分片控制器）服务器通信时才需要取得进展。即使某些副本组中的少数服务器宕机、暂时不可用或运行缓慢，您的实现也必须能够操作（处理请求并能够按需重新配置）。

​		我们需要在提供KV查询服务基础上，在**服务器监视配置更改时启动分片迁移过程**。如果一个副本组失去一个分片，它必须立即停止处理该分片中键的请求，并开始将该分片的数据迁移到接管该分片的副本组。如果一个副本组获得了一个分片，它需要等待之前的所有者发送旧的分片数据后，才能接受该分片的请求。

Shard KV属性定义如下：

```
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
```

### 2.Get和PutAppend RPC实现

在RPC中新增了Key是否隶属于当前分组的判断，其他代码与Lab 4中的实现基本一致；

```
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{OprateType:GET, Key:args.Key, ClientId: nrand(), Version: 1}
	
	templateReply := kv.operateTemplate(op)
	reply.Err = templateReply.Err
	reply.Value = templateReply.Value
}
```

```
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{Key:args.Key,Value: args.Value, ClientId: args.Id, Version: args.Version}
	if args.Op == "Put" {
		op.OprateType = PUT
	} else {
		op.OprateType = APPEND
	}

	templateReply := kv.operateTemplate(op)
	reply.Err = templateReply.Err
}
```

```

func (kv *ShardKV) operateTemplate(op Op) ReplyTemplate{
	reply := ReplyTemplate{}
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return reply
	}

	kv.mu.Lock()

	//检查到来的请求是否当前的group
	if kv.checkWroneShardLocked(op.Key) {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return reply
	}
	kv.mu.Unlock()

	//写日志不要占用锁
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return reply
	}

	kv.mu.Lock()
	ch := make(chan Res, 1)
	kv.waitCh[index] = ch
	kv.mu.Unlock()

	timeout := time.After(1 * time.Second)
	select {
	case commitRes := <- ch:
		commitOp := commitRes.OP
		if commitRes.ERROR == OK || commitRes.ERROR == ErrRepeatRequest{
			
			if commitOp.ClientId == op.ClientId && commitOp.Version == op.Version {
				reply.Err = OK
				if commitOp.OprateType == GET {
					reply.Value = commitRes.Value
				}
			} else {
				reply.Err = ErrWrongLeader
			}
		} else if commitRes.ERROR == ErrNoKey {
			reply.Err = ErrNoKey
			reply.Value = ""
		} else if commitRes.ERROR == ErrWrongGroup {
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
	return reply
}
```

```
//检查Key是否隶属于当前的分组
func (kv *ShardKV) checkWroneShardLocked(key string) bool{
	shard := key2shard(key)
	return kv.curConfig.Num == 0 || kv.curConfig.Shards[shard] != kv.gid
}
```

### 3.Pull Config

#### 3.1 Pull Config

​		每隔一段时间Leader检测是否有比当前Config更新的日志（查询num+1的Config是否存在），如果存在Raft中保存Config，等待更新Config。

```
func (kv *ShardKV) pollNewConfig() {
	for kv.killed() == false {
		_, isleader := kv.rf.GetState()
		
		kv.mu.Lock()
		if isleader && len(kv.comeInShards) == 0 {
			num := kv.curConfig.Num
			//判断是否有比当前Config更新的日志
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
```

#### 3.2 Update Config

3.1中提交Config到Raft中，当Raft提交Config到上层服务器后则开始进行Config更新操作：

1. 首先判断提交来的config是否比当前的config新；
2. 根据新config更新kv.shardValid（Map，判断哪些分片隶属于当前分组），kv.comeInShards （Map结构，判断哪些分片将会从哪个配置被传输过来）， toOutShards （Map结构，判断哪些分片将会被传送出去）；
3. 根据toOutShard数组组织将要被移出的数据到kv.toOutShards（Map结构，configNum->[Shard->[key->value]]）；

```
func (kv *ShardKV) updateConfigChange(cfg shardctrler.Config) {
	kv.mu.Lock()
    defer kv.mu.Unlock()
    
    //only consider newer config
    if cfg.Num <= kv.curConfig.Num { 
        return
    }
    oldCfg := kv.curConfig
	toOutShard := kv.shardValid
	
	//根据新config确定哪些shard仍然隶属于当前config，哪些数据将会从其他group传送过来；
    kv.shardValid, kv.curConfig = make(map[int]bool), cfg
    for shard, gid := range cfg.Shards {
        if gid != kv.gid {continue}
        if  toOutShard[shard] || oldCfg.Num == 0 {
            kv.shardValid[shard] = true
            //更新需要移出的分片
            delete(toOutShard, shard)
        } else {
            kv.comeInShards[shard] = oldCfg.Num
        }
    }
    
    // 根据toOutShard数组去收集需要移动的数据
    if len(toOutShard) > 0 { 
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
```

### 4.Pull Data

#### 4.1 定时pollShardData

Config更新带来的可能是分片数据的移动，Leader需要定期去检查 kv.comeInShards，如果 kv.comeInShards不为空，则去对应的服务器拉去分片数据。

```
func (kv *ShardKV) pollShardData() {
	for kv.killed() == false {
		_, isleader := kv.rf.GetState()
		kv.mu.Lock()	
		if isleader && len(kv.comeInShards) > 0 {
			var wg sync.WaitGroup
			for shard, num := range(kv.comeInShards) {
				wg.Add(1)
				conf := kv.mck.Query(num)
				//查config，获取servers列表
				servers := conf.Groups[conf.Shards[shard]]
				args := PullShardArgs{Shard: shard, ConfigNum: conf.Num}
				Log(dSERVER, "S-%v-%d:pull shard request, oldNum=%v, shard=%v", kv.gid, kv.me,num, shard)
				go func (){
					for _, server := range servers {
						reply := PullShardReply{}
						ck := kv.make_end(server)
						//调用PullShardData RPC,拉取数据
						success := ck.Call("ShardKV.PullShardData", &args, &reply)
						if success && reply.Err == OK {
							//拉取数据后将其写入到Raft
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
```

#### 4.2 PullShardData RPC实现

从对应配置对应分片中拉去数据：

```
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

```

#### 4.3 获取分片数据

分组内KV server应用来自Raft的Log，增加来自转移分片的数据到dataMap中，与此将ConfigNum和Shard记录kv.garbageShards中，后续我们将通过kv.garbageShards来告知数据源头删除数据！

```
func (kv *ShardKV) updateMapAfterPullShard(reply PullShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if reply.ConfigNum != kv.curConfig.Num - 1 {
		return
	}
	
	delete(kv.comeInShards, reply.Shard)

	if _, ok := kv.shardValid[reply.Shard]; !ok{
		//更新map
		for k, v := range(reply.Data) {
			kv.dataMap[k] = v
		}
		for id, seq := range(reply.ClientMap) {
			kv.clientMap[id] = max(seq, kv.clientMap[id])
		}
		//设置分片为合法访问
		kv.shardValid[reply.Shard] = true

		if _, ok := kv.garbageShards[reply.ConfigNum]; !ok {
			kv.garbageShards[reply.ConfigNum] = make(map[int]bool)
		}
		kv.garbageShards[reply.ConfigNum][reply.Shard] = true
	}
	Log(dSERVER, "S-%v-%d:update data, config.Num=%v, shardValid=%v,shard=%v", kv.gid,kv.me, reply.ConfigNum, kv.shardValid,reply.Shard)
}
```

### 5. 数据回收

​		我们在kv.toOutShards中保存了不同配置下需要转移的分片数据，随着时间增长，kv.toOutShards会占用越来越多的空间，因此我们要设计回收无用的数据。

​		每当目的分组接受了来自源分组的分片数据之后，我们都会在目的分组中通知源头分组删除不用的数据，以此节约存储空间。

***注意：***kv.garbageShards变量（Map结构，configNum -> (shard -> bool)）保存了要删除的分片数据信息

#### 5.1 garbageCollecter

垃圾回收器每隔一段时间旧检查是否有无用数据，如果存在，则通过RPC通知数据源头删除数据：

```

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

					go func () {
						for _, server := range servers {
							reply := GarbageCollectReply{}
							ck := kv.make_end(server)
							success := ck.Call("ShardKV.GarbageCollect", &args, &reply)
							if success && reply.Err == OK {
								//使用写入Raft， 然后再删除garbageShards？
								//垃圾数据不存在当前目的分组内，而是存在源头分组内，我们需要通过RPC通知源头分组删除冗余数据
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

```

#### 5.2 GarbageCollect RPC

副本Leader节点删除无用输入Log到Raft：

```
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
	//删除日志行为写入 Raft Log
	reply.Err =  kv.garbageCollect(op)
}
```

Leader节点写入 Log：

```
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
```

#### 5.3 无用数据回收

Raft Log中的消息通知KV Server删除数据：

```
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
```

### 6. Applier实现

```
func (kv *ShardKV) applier() {
	for kv.killed() == false {
		applyMsg := <- kv.applyCh
		if applyMsg.CommandValid {
			if cfg, ok := applyMsg.Command.(shardctrler.Config); ok {
				//更新配置
				kv.updateConfigChange(cfg)
			} else if reply, ok := applyMsg.Command.(PullShardReply); ok {
				//更新数据
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
```



```
func (kv *ShardKV) performOpOnMachine(applyMsg raft.ApplyMsg) {
	op := applyMsg.Command.(Op)
	res := Res{OP: op, Value: ""}

	kv.mu.Lock()	
	if op.OprateType == GC {
		//判断是否为GC
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
```



### BUG:

#### 1.死锁1

代码1：Raft中的锁被Applier占用：

```
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		
		for rf.commitIndex <= rf.lastApplied {
			rf.condApply.Wait()
		}
		...
		rf.mu.Unlock()
	}
}
```

代码2：KV Server中GET、PUT、Append请求时，占用KV Server中的锁，在使用Start方法时请求Raft中的锁：

```
func (kv *ShardKV) operateTemplate(op Op) ReplyTemplate{
	reply := ReplyTemplate{}
	
	kv.mu.Lock()
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return reply
	}

	//检查到来的请求是否当前的group
	if kv.checkWroneShardLocked(op.Key) {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return reply
	}

	//写日志不要占用锁
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return reply
	}

	kv.mu.Unlock()
	...
}

```

代码3：KV Server的Applier也在请求属于KV Server中的锁：

```
func (kv *ShardKV) applier() {
	for kv.killed() == false {
		applyMsg := <- kv.applyCh
		if applyMsg.CommandValid {
			if cfg, ok := applyMsg.Command.(shardctrler.Config); ok {
				//可能请求 KV Server中的锁
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
```

**代码1占用Raft的锁，代码2占用KV Server中的锁并请求Raft中的锁，代码3请求KV Server中的锁单无法获得使得代码1阻塞，因此构成了死锁。**

**解决方法**：代码2不占用KV Server中的锁并请求Raft中的锁：

```
func (kv *ShardKV) operateTemplate(op Op) ReplyTemplate{
	reply := ReplyTemplate{}
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return reply
	}

	kv.mu.Lock()
	//检查到来的请求是否当前的group
	if kv.checkWroneShardLocked(op.Key) {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return reply
	}
	kv.mu.Unlock()

	//写日志不要占用锁
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return reply
	}
	...
}

```

#### 2.死锁2

```
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
```

pollShardData中使用了waitgroup,**要确保kv.mu.Unlock()在wg.Wait()之前执行**。

如果kv.mu.Unlock()在wg.Wait()之后，那么pollShardData就会长时间持有KV Server中的锁，KV Server中的Applier将无法获取锁，而此时底层的Raft的Applier持有锁但无法继续运行；

pollShardData需要从其他的Group拉去数据，其他的节点也可能遇到相同的问题，如果A向B 拉去数据，B向A也向拉去数据，则将造成死锁；

#### 3.垃圾回收设计

为什么GarbageCollect RPC中要等待提交的Log执行结束？

请求GarbageCollect方需要 GarbageCollect的执行结果，执行成功后才能删除记录的数据；



### 优化：

我们kv.garbageShards中删除记录回收的Shard仅仅在Leader中执行，并没有在Follower中执行，可以在设计一个变量保存需要删除的记录，然后再用Raft统一执行。确保Leader和所有Follower有相同的执行结果
