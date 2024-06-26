package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"context"
	"course/labrpc"
	"course/shardctrler"
	"crypto/rand"
	"math/big"
	"time"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
// func key2shard(key string) int {
// 	shard := 0
// 	if len(key) > 0 {
// 		shard = int(key[0])
// 	}
// 	shard %= shardctrler.NShards
// 	return shard
// }

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
	leaderIds map[int]int // 记录 Leader 节点的 id，避免下一次请求的时候去轮询查找 Leader
	// clientID+seqId 确定一个唯一的命令
	clientId int64
	seqId    int64
}

// MakeClerk the tester calls MakeClerk.
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
	ck.leaderIds = make(map[int]int)
	ck.clientId = nrand()
	ck.seqId = 0
	return ck
}

// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	//这里测试出了一个问题，就是当我leave之前做了一个操作让其他server无法连接之后，会出现一个问题
	//具体情况是我shutdown掉大部分节点导致group不可用，然后我leave掉group，但是他还在试之前已经坏掉的group，直接导致他无法正常执行完所有内容，就会被迫timeout
	//但每次get都重新获取一次请求太慢了，做一个updateconfig方法，用于手动update client的config。
	//再次测试发现问题，就是当你无论怎么操作，shutdown掉大部分节点之后再leave，因为该group会无限循环选主导致分片无法进行迁移，所以无论如何访问都会一直返回ErrGroup
	//这个问题目前解决不了
	timeoutchan := time.After(ClientTimeout)
	for {
		select {
		case <-timeoutchan:
			return ErrTimeout
		default:
			//继续执行，不用管
		}
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			if _, exist := ck.leaderIds[gid]; !exist {
				ck.leaderIds[gid] = 0
			}
			oldLeaderId := ck.leaderIds[gid]

			for {
				srv := ck.make_end(servers[ck.leaderIds[gid]])
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
					ck.leaderIds[gid] = (ck.leaderIds[gid] + 1) % len(servers)
					if ck.leaderIds[gid] == oldLeaderId {
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

// PutAppend shared by Put and Append.
// You will have to modify this function.
// add Return
func (ck *Clerk) PutAppend(key string, value string, op string) Err {
	args := PutAppendArgs{
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}
	args.Key = key
	args.Value = value
	args.Op = op
	timeoutchan := time.After(ClientTimeout)

	for {
		select {
		case <-timeoutchan:
			return ErrTimeout
		default:
			//继续执行，不用管
		}
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			if _, exist := ck.leaderIds[gid]; !exist {
				ck.leaderIds[gid] = 0
			}
			oldLeaderId := ck.leaderIds[gid]

			for {
				srv := ck.make_end(servers[ck.leaderIds[gid]])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					ck.seqId++
					return reply.Err
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
				if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
					ck.leaderIds[gid] = (ck.leaderIds[gid] + 1) % len(servers)
					if ck.leaderIds[gid] == oldLeaderId {
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

func (ck *Clerk) Put(key string, value string) Err {
	return ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) Err {
	return ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) GetAll(Shard int) map[string]string {
	args := GetAllArgs{}
	args.Shard = Shard
	timeoutchan := time.After(ClientTimeout * 5)
	for {
		select {
		case <-timeoutchan:
			return nil
		default:
			//继续执行，不用管
		}
		gid := ck.config.Shards[Shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			if _, exist := ck.leaderIds[gid]; !exist {
				ck.leaderIds[gid] = 0
			}
			oldLeaderId := ck.leaderIds[gid]

			for {
				srv := ck.make_end(servers[ck.leaderIds[gid]])
				var reply GetAllReply
				ok := srv.Call("ShardKV.GetAll", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
				if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
					ck.leaderIds[gid] = (ck.leaderIds[gid] + 1) % len(servers)
					if ck.leaderIds[gid] == oldLeaderId {
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

func (ck *Clerk) GetSize(Shard int) int {
	args := GetAllArgs{}
	args.Shard = Shard
	timeoutchan := time.After(ClientTimeout)
	for {
		select {
		case <-timeoutchan:
			return 0
		default:
			//继续执行，不用管
		}
		gid := ck.config.Shards[Shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			if _, exist := ck.leaderIds[gid]; !exist {
				ck.leaderIds[gid] = 0
			}
			oldLeaderId := ck.leaderIds[gid]

			for {
				srv := ck.make_end(servers[ck.leaderIds[gid]])
				var reply GetAllReply
				ok := srv.Call("ShardKV.GetSize", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Size
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
				if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
					ck.leaderIds[gid] = (ck.leaderIds[gid] + 1) % len(servers)
					if ck.leaderIds[gid] == oldLeaderId {
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

func (ck *Clerk) GetClientId() int64 {
	return ck.clientId
}

func (ck *Clerk) GetLeader() map[int]int {

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)

	defer cancel()
	if ctx.Err() == context.DeadlineExceeded {
		// 处理超时情况，超时我们就返回旧的
		return ck.leaderIds
	}
	if len(ck.config.Groups) == 0 {
		//考虑到有可能会出现刚开导致ck.config.Groups还未加载出来
		timeoutDuration := 50 * time.Millisecond // 设置休息时间为50毫秒
		time.Sleep(timeoutDuration)
		ck.config = ck.sm.Query(-1)
	}
	for gid, _ := range ck.config.Groups {
		shard := ck.gidGetShard(gid)
		if shard == -1 {
			panic("error gid -> shard error")
		}
		key := shardtoKey(shard)
		//用这个get方法寻找key的方式更新Leader
		ck.Get(key)
	}

	return ck.leaderIds
}

// use for Check node is ture
// 这里Get的数据不一定存在，我们认为返回只要是OK/ErrNoKey,则代表该节点可达并且是Leader
// 如果返回ErrWrongLeader则认为节点可达但不是leader
func (ck *Clerk) CheckNode(gid, id int) Err {
	timeoutDuration := 300 * time.Millisecond  // 设置超时时间为300毫秒
	timeoutChan := time.After(timeoutDuration) // 创建一个300毫秒后触发的定时器
	for {
		select {
		case <-timeoutChan:
			// 如果定时器触发，跳出循环
			return ErrTimeout
		default:
			// 继续执行循环体
		}
		servers := ck.config.Groups[gid]
		if len(servers) == 0 || id >= len(servers) || servers[id] == "" || ck.gidGetShard(gid) == -1 {
			// ask controler for the latest configuration.
			ck.config = ck.sm.Query(-1)
			continue
		}

		//这俩步是为了找到对应gid的shard，根据shard生成一个key,保证get可以获取对应的group上
		shard := ck.gidGetShard(gid)
		key := shardtoKey(shard)

		srv := ck.make_end(servers[id])
		var reply GetReply
		args := GetArgs{}
		args.Key = key
		ok := srv.Call("ShardKV.Get", &args, &reply)
		if !ok {
			// return ErrSendError//不需要，如果网络不可达就继续重试，直到超时，防止测试中会出现节点活跃但是sendError的情况
			continue
		}
		return reply.Err
	}
}

func (ck *Clerk) gidGetShard(gid int) int {
	for shard, s_gid := range ck.config.Shards {
		if s_gid == gid {
			return shard
		}
	}
	//shard 中没有对应的gid
	return -1
}

func (ck *Clerk) UpdateConfig() {
	ck.config = ck.sm.Query(-1)
}
