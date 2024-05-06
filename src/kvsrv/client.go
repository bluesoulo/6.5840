package kvsrv

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

// import "time"


type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	retryTimes int
	id int64
	ver int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.retryTimes = 1000
	ck.id = nrand()
	ck.ver = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	args := GetArgs{Key: key}
	reply := GetReply{}
	isSuccess := false

	for i := 0; i < ck.retryTimes; i++ {
		isSuccess = ck.server.Call("KVServer.Get", &args, &reply)
		if isSuccess {
			break
		}
	}
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	args := PutAppendArgs{Key: key,
						Value: value,
						Id: ck.id,
						Ver: ck.ver}
	reply := PutAppendReply{}
	isSuccess := false
	
	for i := 0; i < ck.retryTimes; i++ {
		isSuccess = ck.server.Call("KVServer." + op, &args, &reply)
		if isSuccess {
			break
		}
	}

	// if ck.ver != reply.Ver {
	// 	fmt.Printf("ck.ver: %d, reply.Ver: %d,\n", ck.ver, reply.Ver)
	// }

	/*关键的一步，在重复发送的情况也一定要进行version + 1的操作，以确保错误的版本号恢复到正常！*/
	ck.ver = ck.ver + 1

	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
