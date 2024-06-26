package kvraft

import (
	"course/labrpc"
	"crypto/rand"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	Leaderid int
	//确定唯一命令
	ClientId int64
	SeqId    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.Leaderid = 0
	ck.ClientId = nrand()
	ck.SeqId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key: key,
	}
	//突然看懂了，这里想一直轮询整个servers直到有人回复,而不是只轮询一次
	// for i := range ck.servers {
	for {
		reply := &GetReply{}
		ok := ck.servers[ck.Leaderid].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.Leaderid = (ck.Leaderid + 1) % len(ck.servers)
			continue
		}
		// You will have to modify this function.
		return reply.Value
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
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.ClientId,
		SeqId:    ck.SeqId,
	}
	for {
		reply := PutAppendReply{}
		ok := ck.servers[ck.Leaderid].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.Leaderid = (ck.Leaderid + 1) % len(ck.servers)
			continue
		}
		ck.SeqId++
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
