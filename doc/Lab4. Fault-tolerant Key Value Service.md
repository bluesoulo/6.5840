### 1.实验介绍

​		在本实验中，你将使用 Lab 3 中的 Raft 库构建一个容错的键/值存储服务。你的键/值服务将是一个复制状态机，由多个键/值服务器组成，每个服务器维护一个键/值对数据库（与 Lab 2 中类似），但额外使用 Raft 进行复制。只要大多数服务器存活并且能够通信，你的键/值服务应该能够继续处理客户端请求，即使在其他服务器发生故障或网络分区的情况下。完成 Lab 4 后，你将实现 Raft 交互图中所示的所有部分（Clerk、Service 和 Raft）。

客户端将以与 Lab 2 类似的方式与键/值服务交互。特别是，客户端可以向键/值服务发送三种不同的 RPC：

- Put(key, value)：替换数据库中特定键的值
- Append(key, arg)：将 arg 附加到键的值后面（如果键不存在，则将现有值视为空字符串）
- Get(key)：获取键的当前值（如果键不存在则返回空字符串）

键和值都是字符串。请注意，与 Lab 2 不同，Put 和 Append 不应向客户端返回值。每个客户端通过具有 Put/Append/Get 方法的 Clerk 与服务通信。Clerk 管理与服务器的 RPC 交互。

你的服务必须确保 Clerk Get/Put/Append 方法的应用调用是线性化的。如果一次调用一个，Get/Put/Append 方法应该表现得像系统只有一个状态副本一样，每次调用都应该观察到前一系列调用所暗示的状态修改。对于并发调用，返回值和最终状态必须与这些操作按某种顺序逐个执行时的结果相同。如果调用在时间上重叠，则它们是并发的：例如，如果客户端 X 调用 Clerk.Put()，而客户端 Y 调用 Clerk.Append()，然后客户端 X 的调用返回。**一个调用必须观察到在其开始之前完成的所有调用的效果。**

### 3.Client端实现

#### 3.1 Client属性

```
type Clerk struct {
	servers []*labrpc.ClientEnd
	id int64
	ver int
	//缓存LeaderId，方便下次访问
	leaderId int
}
```

**Id和ver的作用是什么？**

***Id用来标识每一个Client，Ver用来表示Client的每一个请求。***

Client端发送RPC请求到Server端口，如果消息丢失，那么客户端的 `ck.server.Call()` 将返回 `false`（更准确地说，`Call()` 会在一个超时间隔内等待回复消息，如果在该时间内没有收到回复，则返回 `false`），那么client可能需要多次发送RPC请求，直到成功为止，每次调用 `Clerk.Put()` 或 `Clerk.Append()` 应该只执行一次，我们需要确保重新发送的请求不会导致服务器执行两次。

Client在发送Append请求到Server端时，在Append请求中包含了Id和Ver，在Server中会记录每一个Client执行的最大请求Ver，如果请求小于Server记录的Client Ver则拒绝请求。

#### 3.2 Get请求

Get请求不会对KV Server造成影响，可以看作是幂等操作，因此不需要使用ClientId和Ver。

Get请求会循环遍历每一个Server服务器，直到得到结果返回。

```
func (ck *Clerk) Get(key string) string {
	
	args := GetArgs{Key: key}
	reply := GetReply{}
	isSuccess := false

	leaderId := ck.leaderId

	for !isSuccess {
		isSuccess = ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		
		if !isSuccess {
			//1.网络原因未能成功接收到RPC请求，访问失败，换一个主机继续请求；
			leaderId = (leaderId + 1) % len(ck.servers)
		} else if reply.Err == ErrWrongLeader {
			//2.接受RPC请求的主机不是Leader换一个主机继续请求；
			leaderId = (leaderId + 1) % len(ck.servers)
			isSuccess = false
		}
	}
	
	ck.leaderId = leaderId		
	if reply.Err == OK || reply.Err == ErrRepeatRequest{
		return reply.Value
	} else {
	//找不到对应的key，返回空字符串
		return ""
	}
}
```

#### 3.3 Put和Append请求

Put和Append请求会对KV Server造成影响，因此必须确保每一个请求只会执行一次；

```
func (ck *Clerk) PutAppend(key string, value string, op string) {
	//增加版本号
	ck.ver = ck.ver + 1
	args := PutAppendArgs{Key: key, Value: value, Id: ck.id, Ver: ck.ver}
	reply := PutAppendReply{}
	isSuccess := false

	leaderId := ck.leaderId
	for !isSuccess {

		isSuccess = ck.servers[leaderId].Call("KVServer."+ op, &args, &reply)

		if !isSuccess {
			leaderId = (leaderId + 1) % len(ck.servers)
		} else if reply.Err == ErrWrongLeader {
			leaderId = (leaderId + 1) % len(ck.servers)
			isSuccess = false
		} 
	}
	//缓存LeaderId，方便下次访问
	ck.leaderId = leaderId
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
```

### 4.Server端实现

#### 4.1 KVServer属性定义

```
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	dataMap map[string]string
	
	// client id 和 version 的映射
	clientMap map[int64]int
	// 跟踪客户端等待的请求结果
    waitCh map[int]chan Res

	persister *raft.Persister
	//记录Raft中提交的日志，被应用到状态机的Log index
	lastAppliedIndex int
}
```

#### 4.2 Get请求

Server端首先判断是否为Leader，如果为Leader则使用Raft记录Log，并使用channel来接受状态机执行结果；

为了避免channel永久阻塞，自定义定时器，定时器自动删超时后删除channel；

```
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	//判断是否为leader
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	//随机生成一个ClientId伪装请求
	op := Op{OprateType:GET, Key:args.Key, ClientId: nrand(), Version: 1}
	//不需要在使用mutex的条件下执行
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	//定义channel，并存储
	ch := make(chan Res, 1)
	kv.waitCh[index] = ch
	kv.mu.Unlock()


	// 设置超时时间 1 秒
    timeout := time.After(1 * time.Second)
	
	//channel中没有接受到输入时阻塞
	select {
	//从channel中取状态机执行结果
	case commitRes := <- ch:
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
		}
	//超时自动删除
	case <- timeout:
		kv.mu.Lock()
		delete(kv.waitCh, index)
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
	}
}

```

#### 4.3 Put 和Append请

Put和Append请求中增加了判断重复请求的代码，如果op.Version <= kv.clientMap[op.ClientId]，则该请求为重复请求，直接返回；

```
func (kv *KVServer) putAndAppend(op * Op, reply *PutAppendReply) {
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()

	if op.Version <= kv.clientMap[op.ClientId] {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	kv.mu.Unlock()
	
	//不需要在使用mutex的条件下执行
	index, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		
		reply.Err = ErrWrongLeader
		return
	}
	
	kv.mu.Lock()
	ch := make(chan Res, 1)
	kv.waitCh[index] = ch
	kv.mu.Unlock()

	// 设置超时时间 1 秒
    timeout := time.After(1 * time.Second)
    //channel中没有接受到输入时阻塞
	select {
	//从channel中取状态机执行结果
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
```

```
func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	//来自Client的ClientId和Version
	op := Op{OprateType:PUT,
		  Key:args.Key,
		  Value: args.Value,
		  ClientId: args.Id,
		  Version: args.Ver,
		}
	kv.putAndAppend(&op, reply)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{OprateType:APPEND,
		Key:args.Key,
		Value: args.Value,
		ClientId: args.Id,
		Version: args.Ver,
	  }
	kv.putAndAppend(&op, reply)
}
```

#### 4.4 Applier实现

Raft中提交的日志，最终都会传送到KV Server Applier中统一执行。Applier函数代码逻辑为：

1. 首先判断来自Raft的applyMsg是否为Snapshot；如果不是则执行2，如果是则执行3。
2. 首先会判断该Log中保存的操作是否为重复请求，如果是则直接过滤请求，否则则执行具体的GET，PUT，APPEND请求，并将执行结果通过channel传送到具体的请求。
3. 此时SnapshotValid为True，则需要读取Snapshot数据到dataMap和clientMap变量中。

**注意：**

```
//GET请求不需要记录访问情况
if op.OprateType == PUT || op.OprateType == APPEND {
	kv.clientMap[op.ClientId] = op.Version
}
```

```
func (kv *KVServer) applier() {
	for kv.killed() == false {
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
						res.Value = value
					} else {
						res.ERROR = ErrNoKey
					}
				}

				//GET请求不需要记录访问情况
				if op.OprateType == PUT || op.OprateType == APPEND {
					kv.clientMap[op.ClientId] = op.Version
				}
			} else {
				res.ERROR = ErrRepeatRequest
				if op.OprateType != PUT && op.OprateType != APPEND {
					res.Value = ""
				}
			}
			//状态机记录最大被应用的logIndex
			kv.lastAppliedIndex = applyMsg.CommandIndex
			//放入日志Index对应的channel中
			if ch, ok := kv.waitCh[applyMsg.CommandIndex]; ok {
				ch <- res
				delete(kv.waitCh, applyMsg.CommandIndex)
			}

			kv.mu.Unlock()
		} else if applyMsg.SnapshotValid {
			//处理Follower InstallSnapshot的情况
			dataMap, clientMap := kv.readSnapshotData(applyMsg.Snapshot)
			
			kv.mu.Lock()
			if dataMap != nil {				
				kv.dataMap = dataMap
				
			}
			if clientMap != nil {
				kv.clientMap = clientMap
			}

			kv.mu.Unlock()
		}
	}
}
```

#### 4.5 Detecter实现

Detecter会不断检测KV Server中RaftStateSize的大小，如果大于maxraftstate，那么就会触发Snapshot操作。

Snapshot时会首先读取当前保存的Snapshot数据（即dataMap、clientMap两个变量），并将lastAppliedIndex之前的Log在当前Snapshot的基础上再次执行一次，保存这时dataMap、clientMap两个变量到磁盘上。

**注意：**

我们在GET请求中使用随机数作为clientID，ver设置为1, GET请求不需要保存clientID和ver。

```
//GET请求不需要记录访问情况，且可以节约存储空间
if op.OprateType == PUT || op.OprateType == APPEND {
	clientMap[op.ClientId] = op.Version
}
```

```
func (kv *KVServer) detecter(){
	for kv.killed() == false {
		if kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= kv.maxraftstate {
			raftState := kv.persister.ReadRaftState()
			log, lastIncludedIndex := kv.readPersist(raftState)

			dataMap, clientMap := kv.readSnapshotData(kv.persister.ReadSnapshot())

			if dataMap == nil {
				dataMap = make(map[string]string)
			}
			if clientMap == nil {
				clientMap = make(map[int64]int)
			}

			kv.mu.Lock()
			lastAppliedIndex := kv.lastAppliedIndex 
			kv.mu.Unlock()

			logIndex := lastAppliedIndex - lastIncludedIndex
			//因为我们是先读取了Log，然后再加锁读取了kv.lastAppliedIndex，
			//没有加所保证之前的Log和kv.lastAppliedIndex是一致的状态
			//读取lastAppliedIndex需要上锁，可能在等待上锁的过程中lastAppliedIndex增大，使得出现了log不包含的元素
			if logIndex >= len(log) {
				logIndex = len(log) - 1
			}
			//在旧的Snapshot上执行保存的Log，然后保存新的Sanptshot(dataMap和clientMap变量)。
			for i := 1; i <= logIndex; i++ {

				op := log[i].Command.(Op)
				
				if clientMap[op.ClientId] < op.Version {
					//GET请求不影响状态机的状态
					if op.OprateType == PUT {
						dataMap[op.Key] = op.Value
					} else if op.OprateType == APPEND {
						dataMap[op.Key] = dataMap[op.Key] + op.Value
					}

					//GET请求不需要记录访问情况，且可以节约存储空间
					if op.OprateType == PUT || op.OprateType == APPEND {
						clientMap[op.ClientId] = op.Version
					}
				}

			}
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(dataMap)
			e.Encode(clientMap)

			Log(dSERVER,"S%d:detect need to save a snapshot, lastIncludeIndex = %d, lastAppliedIndex=%d", kv.me, lastIncludedIndex + logIndex, lastAppliedIndex)
			//这里不能直接使用lastAppliedIndex，因为我们最终并不是切割了lastAppliedIndex - lastIncludedIndex
			kv.rf.Snapshot(lastIncludedIndex + logIndex, w.Bytes())
		}
		time.Sleep(200 * time.Millisecond)
	}
}
```

### BUG

#### 1. Get请求使用随机数模拟ClientId时，clientMap数据增多

Applier执行时clientMap不需要保存Get请求中保存的访问情况，Snapshot时同样的处理。

#### 2.为什么需要持久化状态机中的clientMap

如果不持久化clientMap，主机重启后clientMap将会为空，可能会导致一个请求被多次执行。如果一个PUT请求被发送了多次，旧的请求在正常执行，而此后主机重启，这个PUT请求将会被多次执行。

#### 3.Detecter中的BUG

```
if kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= kv.maxraftstate {
    ...

    logIndex := lastAppliedIndex - lastIncludedIndex
    //因为我们是先读取了Log，然后再加锁读取了kv.lastAppliedIndex，
    //没有加所保证之前的Log和kv.lastAppliedIndex是一致的状态
    //读取lastAppliedIndex需要上锁，可能在等待上锁的过程中lastAppliedIndex增大，使得出现了log不包含的元素
    if logIndex >= len(log) {
        logIndex = len(log) - 1
    }
}
```

因为我们是先读取了Log，然后再加锁读取了kv.lastAppliedIndex，没有加所保证之前的Log和kv.lastAppliedIndex是一致的状态
读取lastAppliedIndex需要上锁，可能在等待上锁的过程中lastAppliedIndex增大，使得出现了log不包含的元素。因此需要做出判断：

```
if logIndex >= len(log) {
    logIndex = len(log) - 1
}
```



