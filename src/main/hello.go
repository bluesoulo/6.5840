package main

import "fmt"

func joinRebalcance(shards [10]int, originGids []int, gids []int) [10]int {
	var newShards [10]int
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
	fmt.Printf("cycleShards=%v\n",cycleShards)
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

	if endGroupNum <= 0 {
		return newShards
	}

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
	//回收Leave的group对应的shard
	for i := 0; i < len(gids); i++ {
		cycleShards = append(cycleShards, groupShardMap[gids[i]]...)
		delete(groupShardMap, gids[i])
	}
	//将回收的shard重新分配
	fmt.Printf("groupShardMap=%v,cycleShards=%v\n",groupShardMap,cycleShards)
	index := 0
	for id,_ := range(groupShardMap) {
		next := 0
		if rem > 0 {
			rem--
			next = avg + 1 - len(groupShardMap[id])
		} else {
			next = avg - len(groupShardMap[id])
		}
		next = index + next
		groupShardMap[id] = append(groupShardMap[id], cycleShards[index:next]...)
		index = next
	}
	fmt.Printf("groupShardMap=%v,cycleShards=%v\n",groupShardMap,cycleShards)
	for id, array := range groupShardMap {
		for i := 0; i < len(array); i++ {
			newShards[array[i]] = id
		}
	}
	return newShards
}

func main(){
	shards := [10]int{1110,1120,1130,1140,1150,1160,1170,1180,1190,1100}
	originGids := []int{1110,1120,1130,1140,1150,1160,1170,1180,1190,1100}
	gids := []int{1110}
	newShards := leaveRebalance(shards,originGids,gids)
	fmt.Printf("newShards=%v\n", newShards)
	fmt.Print("this my first go program!")
}