### 1.实验介绍

在这个实验中，将构建一个键/值存储系统，该系统会将键分片（或分区）到一组副本组中。一个分片是键/值对的一个子集；例如，所有以“a”开头的键可能属于一个分片，所有以“b”开头的键属于另一个分片，等等。分片的目的是为了提高性能。每个副本组只处理少数几个分片的存取操作，这些副本组并行工作；因此，总系统吞吐量（每单位时间的存取操作次数）随着组数的增加而增加。

分片键/值存储系统将包含两个主要组件。**首先是一组副本组。每个副本组负责一部分分片，并使用 Raft 复制协议进行复制。第二个组件是“分片控制器”。分片控制器决定哪个副本组应该服务每个分片；这些信息称为配置。**配置会随着时间的推移而变化。客户端会咨询分片控制器以找到某个键所属的副本组，而副本组会咨询控制器以确定应服务哪些分片。整个系统只有一个分片控制器，使用 Raft 实现为一个容错服务。

分片存储系统必须能够在副本组之间移动分片。一个原因是，有些组可能会变得比其他组更繁忙，因此需要**移动分片以平衡负载**。另一个原因是副本组可能会加入或离开系统：可能会添加新的副本组以增加容量，或者将现有的副本组下线以进行修理或退役。

### 2.分片控制器

Shardctrler（分片控制器）管理一系列编号的配置。每个配置描述了一组副本组以及分片到副本组的分配。每当需要更改此分配时，分片控制器会创建一个包含新分配的配置。键/值客户端和服务器在需要了解当前（或过去）的配置时，会联系分片控制器。

我们需要 完成`shardctrler/common.go` 中描述的 RPC 接口，该接口包括 Join、Leave、Move 和 Query 这四种 RPC：

1. ***Join RPC*** 由管理员用于添加新的副本组。它的参数是一个从唯一的非零副本组标识符（GID）到服务器名称列表的映射集合。分片控制器应通过创建一个包含新副本组的新配置来响应。新配置应尽可能均匀地在所有组中划分分片，并且为实现这一目标，应尽量减少分片的移动。如果 GID 不在当前配置中，分片控制器应允许重复使用 GID（即，允许一个 GID 先 Join，再 Leave，然后再次 Join）。
2. ***Move RPC*** 的参数是一个分片号和一个 GID。分片控制器应创建一个将该分片分配给该组的新配置。Move 的目的是为了让我们测试您的软件。Move 之后的 Join 或 Leave 可能会撤销该 Move，因为 Join 和 Leave 会重新平衡。
3. ***Query RPC*** 的参数是一个配置号。分片控制器应回复具有该编号的配置。如果编号为 -1 或大于已知的最大配置编号，分片控制器应回复最新配置。Query(-1) 的结果应反映分片控制器在接收到 Query(-1) RPC 之前已处理完的所有 Join、Leave 或 Move RPC。
4. ***Leave RPC*** 的参数是先前已加入组的 GID 列表。分片控制器应创建一个不包括这些组的新配置，并将这些组的分片分配给剩余的组。新配置应尽可能均匀地在各组之间划分分片，并应尽量减少分片的移动以实现这一目标。

### 3.RPC接口实现

Join、Move、Query、Leave操作的对应的RPC请求与KV Server的PUT、Append请求实现方法基本一致，都需要首先提交Log到Raft中，Raft确保有一半的服务器保存了该日志后再提交到状态机。

#### 3.1 operateTemplate

```

func (sc *ShardCtrler) operateTemplate(op Op) ReplyTemplate{
	reply := ReplyTemplate{}

	//判断是否为Leader
	_, isleader := sc.rf.GetState()
	if !isleader {
		reply.WrongLeader = true
		return reply
	}

	sc.mu.Lock()
	//加速Query请求
	if op.OprateType == QUERY && op.QueryOp.Num >= 0 && op.QueryOp.Num < len(sc.configs) {
		reply.Config = sc.configs[op.QueryOp.Num]
		reply.Err = OK
		sc.mu.Unlock()
		return reply
	}
	
	//判断是否为重复请求
	if op.Version <= sc.clientMap[op.ClientId] {
		sc.mu.Unlock()
		reply.Err = OK
		reply.WrongLeader = false
		return reply
	}
	sc.mu.Unlock()

	//Raft写日志
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return reply
	}

	//创建channel
	sc.mu.Lock()
	ch := make(chan Res, 1)
	sc.waitCh[index] = ch
	sc.mu.Unlock()

	timeout := time.After(1 * time.Second)
	select {
	//channel等待输入
	case commitRes := <- ch:
		commitOp := commitRes.OP
		if commitRes.ERROR == OK || commitRes.ERROR == ErrRepeatRequest {
			if commitOp.ClientId == op.ClientId && commitOp.Version == op.Version {
				if op.OprateType == QUERY {
					reply.Config = commitRes.config
				}
				reply.Err = OK
			} else {
				reply.WrongLeader = true
			}
		} else {
			reply.WrongLeader = true
		}
	//超时自动删除
	case <- timeout:
		sc.mu.Lock()
		delete(sc.waitCh, index)
		sc.mu.Unlock()
		reply.WrongLeader = true
	}

	return reply
}

```

#### 3.2 Join RPC

```
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{OprateType: JOINT, JoinOP: JoinOP{Servers: args.Servers},ClientId: args.ClientId, Version: args.Version}
	
	templateReply := sc.operateTemplate(op);
	reply.Err = templateReply.Err
	reply.WrongLeader = templateReply.WrongLeader
}
```

#### 3.3 Leave RPC

```
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{OprateType: LEAVE, LeaveOp: LeaveOp{GIDs: args.GIDs}, ClientId: args.ClientId, Version: args.Version}
	templateReply := sc.operateTemplate(op);
	reply.Err = templateReply.Err
	reply.WrongLeader = templateReply.WrongLeader
}
```

#### 3.4 Move RPC

```
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{OprateType: MOVE, MoveOP: MoveOP{Shard: args.Shard, GID: args.GID}, ClientId: args.ClientId, Version: args.Version}
	templateReply := sc.operateTemplate(op);
	reply.Err = templateReply.Err
	reply.WrongLeader = templateReply.WrongLeader
}
```

#### 3.5 Query RPC

```
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {

	op := Op{OprateType: QUERY, QueryOp: QueryOp{Num: args.Num}, ClientId: nrand(), Version: 1}
	templateReply := sc.operateTemplate(op);
	reply.Err = templateReply.Err
	reply.WrongLeader = templateReply.WrongLeader
	reply.Config = templateReply.Config
}
```

### 4. 具体操作实现

#### 4.1 Join

在配置中每一个组控制一部分分片，加入新的组后为这些新的组分配分片。需要注意的是：***要确保分配均衡，且尽量减少分片的移动***

```
func (sc *ShardCtrler) joinLocked(args *JoinOP) {
	lastConfig := sc.configs[len(sc.configs) - 1]
	//Shards数组：保存了每一个Shard对应的Group
	oldShards := lastConfig.Shards
	//Groups：group id和每一个Group下的副本服务器映射
	oldgroups := lastConfig.Groups

	originGids := make([]int, 0)
	newGids := make([]int, 0)

	for id, _ := range oldgroups {
		originGids = append(originGids, id)
	}
	for id, _ := range args.Servers {
		newGids = append(newGids, id)
	}
	//为新的分组分配分片，且要确保分片尽量少移动
	newShards := joinRebalcance(oldShards,originGids,newGids)
	newGroups := copyMap(oldgroups)
	for id, names := range args.Servers {
		newGroups[id] = names
	}

	config := Config{Num: lastConfig.Num + 1,
					Shards: newShards,
					Groups: newGroups,
					}
	sc.configs = append(sc.configs, config)
}
```

**joinRebalcance 步骤**：

1. 首先计算出增加分组后，组内分片数目的分布情况。如：假设有10个分片，当前有2个组，Join操作新增了2个分组，则新增前分片的分布为：5，5，新增之后分片的分布为：3，3，2，2；

2. 对比新增前后的分片分布情况，回收旧分组的分片：分组1收回2个分片，分组2收回2个分片，共收回4个；

3. 然后将收回的分片重新分配给新的分组；

   ***注意：***

   1. originGids通过遍历GidsMap方式获得，由于Map的特性使得通过遍历Gid Map得到的originGids顺序不一致，为了确保后续回收到的片序列相同；

   2. 同样对于gids我们也需要排序确保后续在分配时顺序一致；

```
func joinRebalcance(shards [10]int, originGids []int, gids []int) [10]int {
	var newShards [10]int
	//originGids通过遍历Gids方式获得，由于Map的特性使得得到的gids顺序不一致，我们要确保顺序一致！
	sort.Ints(originGids)
	//gids同样的道理
	sort.Ints(gids)
	if len(originGids) == 0 {
		for i := 0; i < len(shards); i++ {
			index := i % len(gids)
			newShards[i] = gids[index]
		}
		return newShards
	}

	endGroupNum := len(originGids) + len(gids)

	avg := len(shards) / endGroupNum
	rem := len(shards) % endGroupNum

	//统计旧gid和shard的映射
	groupShardMap := make(map[int][]int)
	//遍历方式决定groupShardMap[shards[i]]中保存的切片不用排序
	for i := 0; i < len(shards); i++ {
		if _, exists := groupShardMap[shards[i]]; exists {
			groupShardMap[shards[i]] = append(groupShardMap[shards[i]], i)
		} else {
			groupShardMap[shards[i]] = []int{i}
		}
	}

	cycleShards := make([]int, 0)
	//回收gid中多余的shard
	for i := 0; i < len(originGids); i++ {
		if rem > 0 {
			if avg < len(groupShardMap[originGids[i]]) {
				rem--
				cycleShards = append(cycleShards, groupShardMap[originGids[i]][avg+1:]...)
				groupShardMap[originGids[i]] = groupShardMap[originGids[i]][:avg+1]
			} else {
				cycleShards = append(cycleShards, groupShardMap[originGids[i]][avg:]...)
				groupShardMap[originGids[i]] = groupShardMap[originGids[i]][:avg]
			}
		} else {
			cycleShards = append(cycleShards, groupShardMap[originGids[i]][avg:]...)
			groupShardMap[originGids[i]] = groupShardMap[originGids[i]][:avg]
		}
	}
	
	//对回收的切片排序，以确保所有节点得到的cycleShards一致
	// sort.Ints(cycleShards)
	
	//重新分配多余的shard,且gids已经经过我们排序
	for i,j := 0,0; i < len(gids); i++ {
		if rem > 0 {
			rem--
			groupShardMap[gids[i]] = cycleShards[j:j + avg + 1]
			j = j + avg + 1
		} else {
			groupShardMap[gids[i]] = cycleShards[j:j + avg]
			j = j + avg
		}
	}

	for id, array := range groupShardMap {
		for i := 0; i < len(array); i++ {
			newShards[array[i]] = id
		}
	}
	return newShards
}
```

#### 4.2 Leave 操作

```
func (sc *ShardCtrler) leaveLocked(args *LeaveOp) {
	lastConfig := sc.configs[len(sc.configs) - 1]
	oldShards := lastConfig.Shards
	oldgroups := lastConfig.Groups

	newGroups := copyMap(oldgroups)

	originGids := make([]int, 0)
	for id := range newGroups {
		originGids = append(originGids, id)
		
	}

	for _, id := range args.GIDs {
		delete(newGroups, id)
	}
	//分组删除后，分片重平衡
	newShards := leaveRebalance(oldShards, originGids, args.GIDs)
	
	config := Config{Num: lastConfig.Num + 1,
		Shards: newShards,
		Groups: newGroups,
		}
	sc.configs = append(sc.configs, config)
}
```

**leaveRebalcance 步骤**：

1. 首先计算出删除分组后，组内分片数目的分布情况。如：假设有10个分片，当前有3个组，Join操作删除了2个分组，删除之前分片的分布为：3，3，2，2；则删除后分片的分布为：5，5，

2. 对比删除前后的分片分布情况，回收旧分组的分片：收回后两个分组的4个分片；

3. 然后将收回的分片重新分配给其他分组（排序的原因是确保其他的副本节点得到一致的结果）；

   ***注意：***

   1. 开始前对gids排序，确保收回时访问gids顺序一致；
   2. 对newGids排序，确保再分配时访问顺序一致；

```
func leaveRebalance(shards [10]int, originGids []int, gids []int) [10] int {
	var newShards [10]int
	endGroupNum := len(originGids) - len(gids)
	
	//开始前对gids排序，确保收回时访问gids顺序一致；
	sort.Ints(gids)

	if endGroupNum <= 0 {
		return newShards
	}

	avg := len(shards) / endGroupNum
	rem := len(shards) % endGroupNum

	groupShardMap := make(map[int][]int)
	for i := 0; i < len(originGids); i++ {
		groupShardMap[originGids[i]] = make([]int, 0)
	}
	//统计旧gid和shard的映射
	for i := 0; i < len(shards); i++ {
		groupShardMap[shards[i]] = append(groupShardMap[shards[i]], i)
	}

	cycleShards := make([]int, 0)
	//回收Leave的group对应的shard
	for i := 0; i < len(gids); i++ {
		cycleShards = append(cycleShards, groupShardMap[gids[i]]...)
		delete(groupShardMap, gids[i])
	}
	//对回收的切片排序，以确保所有节点得到的cycleShards一致
	//sort.Ints(cycleShards)
	
	//重置rem到正确的数值
	for id, _ := range(groupShardMap) {
		//已经达到avg + 1个shards的group删除
		if len(groupShardMap[id]) == avg + 1 {
			rem--
			delete(groupShardMap, id)
		}
	}

	newGids := make([]int, 0)
	for id,_ := range(groupShardMap) {
		newGids = append(newGids, id)
	}
	//对newGids排序，确保再分配时访问顺序一致；
	sort.Ints(newGids)

	index := 0
	for _,id := range(newGids) {
		gap := 0
		if rem > 0 {
			//rem个group能够有富余
			if len(groupShardMap[id]) < avg + 1 {
				rem--
				gap = avg + 1 - len(groupShardMap[id])
			}
		} else {
			gap = avg - len(groupShardMap[id])
		}
		next := index + gap
		groupShardMap[id] = append(groupShardMap[id], cycleShards[index:next]...)
		index = next
	}

	for i:= 0; i < len(shards); i++ {
		newShards[i] = shards[i]
	}
	for id, array := range groupShardMap {
		for i := 0; i < len(array); i++ {
			newShards[array[i]] = id
		}
	}
	return newShards
}
```

#### 4.3 Move操作

```
func (sc *ShardCtrler) moveLocked(args *MoveOP) {
	lastConfig := sc.configs[len(sc.configs) - 1]
	oldShards := lastConfig.Shards
	oldgroups := lastConfig.Groups

	newShards := copyArray(oldShards)

	newGroups := copyMap(oldgroups)
	#move移动分片
	newShards[args.Shard] = args.GID

	config := Config{Num: lastConfig.Num + 1,
		Shards: newShards,
		Groups: newGroups,
		}
	sc.configs = append(sc.configs, config)
}
```

#### 4.4 Query操作

```
func (sc *ShardCtrler) queryLocked(args *QueryOp) Config{
	// Your code here.
	size := len(sc.configs)
	// sc.printNum()
	if args.Num >= size || args.Num < 0{
		return sc.configs[size - 1]
	} else {
		return sc.configs[args.Num]
	}
}
```

### 5.Applier实现

```
func (sc *ShardCtrler) applier() {
	for {
		applyMsg := <- sc.applyCh
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)
			res := Res{OP: op}
			
			sc.mu.Lock()
			if sc.clientMap[op.ClientId] < op.Version {
				res.ERROR = OK
				if op.OprateType == JOINT {
					sc.joinLocked(&op.JoinOP)
				} else if op.OprateType == LEAVE {
					sc.leaveLocked(&op.LeaveOp)
				} else if op.OprateType == QUERY {
					config := sc.queryLocked(&op.QueryOp)
					res.config = config
				} else {
					sc.moveLocked(&op.MoveOP)
				}
				//Query操作不做记录
				if (op.OprateType != QUERY) {
					sc.clientMap[op.ClientId] = op.Version
				}
			} else {
				res.ERROR = ErrRepeatRequest
			}

			if ch, ok := sc.waitCh[applyMsg.CommandIndex]; ok{
				ch <- res
				delete(sc.waitCh, applyMsg.CommandIndex)
			}
			sc.mu.Unlock()
		}
	}
}
```



### BUG

#### 1.Join重平衡时为什么需要排序？

排序originGids是为了回收分片的切片内部顺序一致；

排序gids是为了分配顺序保证一致；

```
func joinRebalcance(shards [10]int, originGids []int, gids []int) [10]int {
	var newShards [10]int
	//originGids通过遍历Gids方式获得，由于Map的特性使得得到的gids顺序不一致，我们要确保顺序一致！
	sort.Ints(originGids)
	//gids同样的道理
	sort.Ints(gids)
	...
}
```

#### 2.Leave重平衡时为什么需要排序？

排序gids是为了回收分片的切片内部顺序一致；

排序newGids是为了分配顺序保证一致；

```
func leaveRebalance(shards [10]int, originGids []int, gids []int) [10] int {
	
	...
	sort.Ints(gids)
	...
	sort.Ints(newGids)

	index := 0
	for _,id := range(newGids) {
		gap := 0
		if rem > 0 {
			//rem个group能够有富余
			if len(groupShardMap[id]) < avg + 1 {
				rem--
				gap = avg + 1 - len(groupShardMap[id])
			}
		} else {
			gap = avg - len(groupShardMap[id])
		}
		next := index + gap
		groupShardMap[id] = append(groupShardMap[id], cycleShards[index:next]...)
		index = next
	}
	...
}

```

