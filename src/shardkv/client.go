package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"course/labrpc"
	"course/shardctrler"
	"crypto/rand"
	"math/big"
	"time"
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.

	LeaderIds map[int]int //groups->Leaderid
	ClientId  int64
	SeqId     int64
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.

	ck.LeaderIds = make(map[int]int)
	ck.ClientId = nrand()
	ck.SeqId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			if _, exist := ck.LeaderIds[gid]; !exist {
				ck.LeaderIds[gid] = 0
			}
			oldLeaderId := ck.LeaderIds[gid]
			for {
				srv := ck.make_end(servers[ck.LeaderIds[gid]])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
				if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
					ck.LeaderIds[gid] = (ck.LeaderIds[gid] + 1) % len(servers)
					//如果轮询了一圈还找不到，就退出然后等待新配置进入之后再进行操作
					if oldLeaderId == ck.LeaderIds[gid] {
						break
					}
					continue
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.ClientId
	args.SeqId = ck.SeqId

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			if _, exist := ck.LeaderIds[gid]; !exist {
				ck.LeaderIds[gid] = 0
			}
			oldLeaderId := ck.LeaderIds[gid]
			for {
				srv := ck.make_end(servers[ck.LeaderIds[gid]])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					ck.SeqId++
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
				if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
					ck.LeaderIds[gid] = (ck.LeaderIds[gid] + 1) % len(servers)
					//如果轮询了一圈还找不到，就退出然后等待新配置进入之后再进行操作
					if oldLeaderId == ck.LeaderIds[gid] {
						break
					}
					continue
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
