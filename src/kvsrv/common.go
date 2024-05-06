package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Id int64  //为每一个client 设置一个随机id
	Ver int64 //请求的版本
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Value string
	Ver int64
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}
