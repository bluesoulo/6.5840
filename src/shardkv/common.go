package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrRepeatRequest = "ErrRepeatRequest"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Id int64  //为每一个client 设置一个随机id
	Version int //请求的版本
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id int64  //为每一个client 设置一个随机id
	Version int //请求的版本
}

type GetReply struct {
	Err   Err
	Value string
}

type PullShardArgs struct{
	Shard int
	ConfigNum int
}

type PullShardReply struct{
	Shard       int
	ConfigNum   int
	Data map[string]string
	ClientMap map[int64]int
	Err Err
}

type GarbageCollectArgs struct {
	Shard int
	ConfigNum int
}

type GarbageCollectReply struct {
	Shard       int
	ConfigNum   int
	Err Err
}