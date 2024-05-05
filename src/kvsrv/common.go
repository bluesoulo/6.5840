package kvsrv

import "os"
import "strconv"

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}

func kvServerSock() string {
	s := "/var/tmp/5840-kv-sever-"
	s += strconv.Itoa(os.Getuid())
	return s
}