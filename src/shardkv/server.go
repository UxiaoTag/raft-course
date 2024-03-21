package shardkv

import (
	"bytes"
	"course/labgob"
	"course/labrpc"
	"course/raft"
	"course/shardctrler"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead           int32
	lastApplied    int
	shards         map[int]*MemoryKVStateMachine
	notifyCh       map[int]chan *OpReply
	duplicateTable map[int64]lastOperationInfo
	currentConfig  shardctrler.Config //当前的配置
	prevConfig     shardctrler.Config //上一份配置
	mck            *shardctrler.Clerk //请求客户端
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//判断是否是负责的分片，否则直接返回
	kv.mu.Lock()
	if !kv.matchGroup(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(RaftCommand{
		CmdType: ClientOpertion,
		Data: Op{
			Key:    args.Key,
			OpType: OpGet,
		},
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//等待结果
	kv.mu.Lock()
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.Value = result.Value
		reply.Err = result.Err
	//该处time.After()返回一个通道，这个通道将在指定的时间后发送一个值（通常是nil），然后关闭。当通道关闭时，select语句中的相应case分支会被执行。所以这句话就是一个计时器
	case <-time.After(ClientRequsetTimeout):
		reply.Err = ErrTimeout
	}
	//异步删除通知的通道，因为是index产生的，所以通道唯一，用完要删
	go func() {
		kv.mu.Lock()
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

// 如果重复发送过返回true,这里seqId如果<=存储中的info.seqId，说明这条命令是在之前就执行过的。
func (kv *ShardKV) requestDuplicated(clientId, seqId int64) bool {
	info, ok := kv.duplicateTable[clientId]
	return ok && seqId <= info.SeqId
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	//判断是否是负责的分片，否则直接返回
	if !kv.matchGroup(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	//判断是否执行过
	if kv.requestDuplicated(args.ClientId, args.SeqId) {
		// 如果是重复请求，直接返回结果
		reply.Err = kv.duplicateTable[args.ClientId].Reply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	//调用raft,请求存储到raft日志并进行同步
	index, _, isLeader := kv.rf.Start(RaftCommand{
		CmdType: ClientOpertion,
		Data: Op{
			Key:      args.Key,
			Value:    args.Value,
			OpType:   getOpType(args.Op),
			ClientId: args.ClientId,
			SeqId:    args.SeqId,
		},
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//等待结果
	kv.mu.Lock()
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()
	select {
	case result := <-notifyCh:
		reply.Err = result.Err
	case <-time.After(ClientRequsetTimeout):
		reply.Err = ErrTimeout
	}
	//异步删除这些通道，因为是index产生的，所以通道唯一，用完要删
	go func() {
		kv.mu.Lock()
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.lastApplied = 0
	kv.dead = 0
	kv.shards = make(map[int]*MemoryKVStateMachine)
	kv.notifyCh = make(map[int]chan *OpReply)
	kv.duplicateTable = make(map[int64]lastOperationInfo)
	kv.currentConfig = shardctrler.DefaultConfig()
	kv.prevConfig = shardctrler.DefaultConfig()

	//read snapshot
	kv.restoreSnapShot(persister.ReadSnapshot())

	// You may need initialization code here.
	go kv.applyTicker()
	go kv.fetchConfigTicker()
	go kv.shardMigrationTicker()
	go kv.shardGCTicker()

	return kv
}
func (kv *ShardKV) applyToMemoryKVStateMachine(op Op) *OpReply {
	var value string
	var Err Err
	shardid := key2shard(op.Key)
	switch op.OpType {
	case OpGet:
		value, Err = kv.shards[shardid].Get(op.Key)
	case OpPut:
		Err = kv.shards[shardid].Put(op.Key, op.Value)
	case OpAppend:
		Err = kv.shards[shardid].Append(op.Key, op.Value)
	}
	return &OpReply{Value: value, Err: Err}
}

func (kv *ShardKV) getNotifyChannel(index int) chan *OpReply {
	if _, ok := kv.notifyCh[index]; !ok {
		kv.notifyCh[index] = make(chan *OpReply, 1)
	}
	return kv.notifyCh[index]
}

// 这个channel是唯一的(由日志的index决定，所以用完要释放)
func (kv *ShardKV) removeNotifyChannel(index int) {
	delete(kv.notifyCh, index)
}

func (kv *ShardKV) makeSnapShot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shards)
	e.Encode(kv.duplicateTable)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *ShardKV) restoreSnapShot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var shards map[int]*MemoryKVStateMachine
	var dupTable map[int64]lastOperationInfo
	if err := d.Decode(&shards); err != nil {
		panic(fmt.Sprintf("restore MemoryKVStateMachine Error %v", err))
	}
	kv.shards = shards
	if err := d.Decode(&dupTable); err != nil {
		panic(fmt.Sprintf("restore duplicateTable Error %v", err))
	}
	kv.duplicateTable = dupTable
}
