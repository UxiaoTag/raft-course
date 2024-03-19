package shardctrler

//
// Shardctrler clerk.
//

import (
	"course/labrpc"
	"crypto/rand"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	LeaderId int
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
	// Your code here.
	ck.LeaderId = 0
	ck.ClientId = nrand()
	ck.SeqId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	for {
		// try each known server.
		var reply QueryReply
		ok := ck.servers[ck.LeaderId].Call("ShardCtrler.Query", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
			continue
		}
		return reply.Config
	}
	// time.Sleep(100 * time.Millisecond)
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.ClientId
	args.SeqId = ck.SeqId
	for {
		// try each known server.
		var reply JoinReply
		ok := ck.servers[ck.LeaderId].Call("ShardCtrler.Join", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
			continue
		}
		ck.SeqId++
		return
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.ClientId
	args.SeqId = ck.SeqId
	for {
		// try each known server.
		var reply LeaveReply
		ok := ck.servers[ck.LeaderId].Call("ShardCtrler.Leave", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
			continue
		}
		ck.SeqId++
		return
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.ClientId
	args.SeqId = ck.SeqId
	for {
		// try each known server.
		var reply MoveReply
		ok := ck.servers[ck.LeaderId].Call("ShardCtrler.Move", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
			continue
		}
		ck.SeqId++
		return
	}
}
