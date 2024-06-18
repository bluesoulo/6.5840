package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	clientMap map[int64]int
	waitCh map[int]chan Res
	olDdataMap map[int64]Config
}

type Res struct {
	OP Op
	config Config
	ERROR string
}

type OPType int
const (
	JOINT OPType = iota
	LEAVE
	MOVE
	QUERY
)

type Op struct {
	// Your data here.
	OprateType OPType

	JoinOP JoinOP
	LeaveOp LeaveOp
	MoveOP MoveOP
	QueryOp QueryOp

	ClientId int64
	Version int
}

func copyMap(origin map[int][]string)map[int][]string {
	newGroups := make(map[int][]string, 0)
	for k, v := range origin {
		newGroups[k] = v
	}
	return newGroups
}

func copyArray(origin [10]int) [10]int{
	var newArray [10]int
	for i := 0; i < len(origin); i++ {
		newArray[i] = origin[i]
	}
	return newArray
}

func joinRebalcance(shards [10]int, originGids []int, gids []int) [10]int {
	var newShards [10]int
	sort.Ints(originGids)
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

	groupShardMap := make(map[int][]int)
	//统计旧gid和shard的映射
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
	sort.Ints(cycleShards)
	//重新分配多余的shard
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

func leaveRebalance(shards [10]int, originGids []int, gids []int) [10] int {
	var newShards [10]int
	endGroupNum := len(originGids) - len(gids)

	sort.Ints(originGids)
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
	sort.Ints(cycleShards)
	
	//将回收的shard重新分配
	for id, _ := range(groupShardMap) {
		//已经达到avg + 1个shards的group删除
		if len(groupShardMap[id]) == avg + 1 {
			rem--
			delete(groupShardMap, id)
		}
	}

	//重新排序，确保访问gid顺序一致
	newGids := make([]int, 0)
	for id,_ := range(groupShardMap) {
		newGids = append(newGids, id)
	}
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

func (sc *ShardCtrler) joinLocked(args *JoinOP) {
	lastConfig := sc.configs[len(sc.configs) - 1]
	oldShards := lastConfig.Shards
	oldgroups := lastConfig.Groups

	originGids := make([]int, 0)
	newGids := make([]int, 0)

	for id, _ := range oldgroups {
		originGids = append(originGids, id)
	}
	for id, _ := range args.Servers {
		newGids = append(newGids, id)
	}
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


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{OprateType: JOINT, JoinOP: JoinOP{Servers: args.Servers},ClientId: args.ClientId, Version: args.Version}
	// Your code here.
	sc.mu.Lock()

	if args.Version <= sc.clientMap[args.ClientId] {
		sc.mu.Unlock()
		reply.Err = OK
		reply.WrongLeader = false
		return
	}

	index, _, isLeader := sc.rf.Start(op)

	if !isLeader {
		sc.mu.Unlock()
		reply.WrongLeader = true
		return
	}

	ch := make(chan Res, 1)
	sc.waitCh[index] = ch

	sc.mu.Unlock()

	timeout := time.After(1 * time.Second)
	select {
	case commitRes := <- ch:
		commitOp := commitRes.OP
		if commitRes.ERROR == OK || commitRes.ERROR == ErrRepeatRequest {
			if commitOp.ClientId == args.ClientId && commitOp.Version == args.Version {
				reply.Err = OK
			} else {
				reply.WrongLeader = true
			}
		} else {
			reply.WrongLeader = true
		}
	case <- timeout:
		sc.mu.Lock()
		delete(sc.waitCh, index)
		sc.mu.Unlock()
		reply.WrongLeader = true
	}
}

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
	newShards := leaveRebalance(oldShards, originGids, args.GIDs)
	
	config := Config{Num: lastConfig.Num + 1,
		Shards: newShards,
		Groups: newGroups,
		}
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{OprateType: LEAVE, LeaveOp: LeaveOp{GIDs: args.GIDs}, ClientId: args.ClientId, Version: args.Version}
	sc.mu.Lock()
	if args.Version <= sc.clientMap[args.ClientId] {
		sc.mu.Unlock()
		reply.Err = OK
		reply.WrongLeader = false
		return
	}

	index, _, isLeader := sc.rf.Start(op)

	if !isLeader {
		sc.mu.Unlock()
		reply.WrongLeader = true
		return
	}

	ch := make(chan Res, 1)
	sc.waitCh[index] = ch

	sc.mu.Unlock()

	timeout := time.After(1 * time.Second)
	select {
	case commitRes := <- ch:
		commitOp := commitRes.OP
		if commitRes.ERROR == OK || commitRes.ERROR == ErrRepeatRequest {
			if commitOp.ClientId == args.ClientId && commitOp.Version == args.Version {
				reply.Err = OK
			} else {
				reply.WrongLeader = true
			}
		} else {
			reply.WrongLeader = true
		}
	case <- timeout:
		sc.mu.Lock()
		delete(sc.waitCh, index)
		sc.mu.Unlock()
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) moveLocked(args *MoveOP) {
	lastConfig := sc.configs[len(sc.configs) - 1]
	oldShards := lastConfig.Shards
	oldgroups := lastConfig.Groups

	newShards := copyArray(oldShards)

	newGroups := copyMap(oldgroups)
	newShards[args.Shard] = args.GID

	config := Config{Num: lastConfig.Num + 1,
		Shards: newShards,
		Groups: newGroups,
		}
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{OprateType: MOVE, MoveOP: MoveOP{Shard: args.Shard, GID: args.GID}, ClientId: args.ClientId, Version: args.Version}
	sc.mu.Lock()

	if args.Version <= sc.clientMap[args.ClientId] {
		sc.mu.Unlock()
		reply.Err = OK
		reply.WrongLeader = false
		return
	}

	index, _, isLeader := sc.rf.Start(op)

	if !isLeader {
		sc.mu.Unlock()
		reply.WrongLeader = true
		return
	}

	ch := make(chan Res, 1)
	sc.waitCh[index] = ch

	sc.mu.Unlock()

	timeout := time.After(1 * time.Second)
	select {
	case commitRes := <- ch:
		commitOp := commitRes.OP
		if commitRes.ERROR == OK || commitRes.ERROR == ErrRepeatRequest {
			if commitOp.ClientId == args.ClientId && commitOp.Version == args.Version {
				reply.Err = OK
			} else {
				reply.WrongLeader = true
			}
		} else {
			reply.WrongLeader = true
		}
	case <- timeout:
		sc.mu.Lock()
		delete(sc.waitCh, index)
		sc.mu.Unlock()
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) printNum() {
	array := make([]int, 0)
	for i := 0; i < len(sc.configs); i++ {
		array = append(array, sc.configs[i].Num)
	}
	
}

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

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{OprateType: QUERY, QueryOp: QueryOp{Num: args.Num}, ClientId: args.ClientId, Version: args.Version}

	sc.mu.Lock()

	if args.Version <= sc.clientMap[args.ClientId] {
		sc.mu.Unlock()
		reply.Config = sc.olDdataMap[args.ClientId]
		reply.Err = OK
		return
	}

	index, _, isLeader := sc.rf.Start(op)

	if !isLeader {
		sc.mu.Unlock()
		reply.WrongLeader = true
		return
	}

	ch := make(chan Res, 1)
	sc.waitCh[index] = ch

	sc.mu.Unlock()

	timeout := time.After(1 * time.Second)
	select {
	case commitRes := <- ch:
		// Log(dLeader, "S%d: -res3=%v", kv.me, commitRes)
		commitOp := commitRes.OP
		if commitRes.ERROR == OK || commitRes.ERROR == ErrRepeatRequest{
			if commitOp.ClientId == args.ClientId && commitOp.Version == args.Version {
				reply.Err = OK
				sc.mu.Lock()
				reply.Config = commitRes.config
				// fmt.Printf("S%d:Query res,config=%v\n",sc.me, reply.Config)
				sc.mu.Unlock()
			} else {
				reply.WrongLeader = true
			}
		}
	case <- timeout:
		sc.mu.Lock()
		// Log(dDrop, "S%d:time out! delete index=%d", kv.me, index)
		delete(sc.waitCh, index)
		sc.mu.Unlock()
		reply.WrongLeader = true
	}

}

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
					sc.olDdataMap[op.ClientId] = config
				} else {
					sc.moveLocked(&op.MoveOP)
				}
				sc.clientMap[op.ClientId] = op.Version
				Log(dLeader,"S%d:operate=%v, groups=%v",sc.me, op.OprateType,sc.configs[len(sc.configs)-1].Groups)
			} else {
				res.ERROR = ErrRepeatRequest
				if op.OprateType == QUERY {
					res.config = sc.olDdataMap[op.ClientId]
				}
			}

			if ch, ok := sc.waitCh[applyMsg.CommandIndex]; ok{
				ch <- res
				delete(sc.waitCh, applyMsg.CommandIndex)
			}
			sc.mu.Unlock()
		}
	}
}


// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	sc.waitCh = make(map[int]chan Res)
	sc.clientMap = make(map[int64]int)
	sc.olDdataMap = make(map[int64]Config)

	go sc.applier()

	return sc
}
