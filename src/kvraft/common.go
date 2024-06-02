package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"	
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Id int64  //为每一个client 设置一个随机id
	Ver int //请求的版本
}

type PutAppendReply struct {
	Err Err
	LeaderId int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id int64  //为每一个client 设置一个随机id
	Ver int //请求的版本
}

type GetReply struct {
	Err   Err
	Value string
	LeaderId int
}
