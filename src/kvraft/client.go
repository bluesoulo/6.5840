package kvraft

import (
	crand "crypto/rand"
	"math/big"
	// mrand "math/rand"
	"6.5840/labrpc"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id int64
	ver int

	leaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = nrand()
	ck.ver = 0
	ck.leaderId = 0
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.ver = ck.ver + 1
	args := GetArgs{Key: key, Id: ck.id, Ver: ck.ver}
	reply := GetReply{}
	isSuccess := false

	Log(dClient,"%d:need to perform! ver=%d, op=Get, key=%s",ck.id, ck.ver, key)
	leaderId := ck.leaderId

	for !isSuccess {
		isSuccess = ck.servers[leaderId].Call("KVServer.Get", &args, &reply)

		if !isSuccess {
			// Log(dClient, "%v:request Server=%v fail.", ck.id,leaderId)
			leaderId = (leaderId + 1) % len(ck.servers)
		} else if reply.Err == ErrWrongLeader {
			// Log(dClient, "%v:request Server=%v fail.", ck.id,leaderId)
			leaderId = (leaderId + 1) % len(ck.servers)
			isSuccess = false
		}
	}
	Log(dClient,"%d: ver=%d, op=Get, key=%s, res=%s", ck.id, ck.ver, key, reply.Value)
	
	ck.leaderId = leaderId
		
	if reply.Err == OK || reply.Err == ErrRepeatRequest{
		return reply.Value
	} else {
		return ""
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.ver = ck.ver + 1
	args := PutAppendArgs{Key: key, Value: value, Id: ck.id, Ver: ck.ver}
	reply := PutAppendReply{}
	isSuccess := false
	Log(dClient,"%d:need to perform! ver=%d, op=%s, key=%s, value=%s",ck.id, ck.ver, op, key, value)

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
	Log(dClient,"%d: ver=%d, op=%s, key=%s, value=%s", ck.id, ck.ver, op, key, value)
	ck.leaderId = leaderId
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
