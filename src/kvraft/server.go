package kvraft

import (
	"course/labgob"
	"course/labrpc"
	"course/raft"
	"sync"
	"sync/atomic"
	"time"
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied          int
	MemoryKVStateMachine *MemoryKVStateMachine
	notifyCh             map[int]chan *OpReply
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{
		Key: args.Key,
		// Value: reply.Value,
		OpType: OpGet,
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

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{
		Key:    args.Key,
		Value:  args.Value,
		OpType: getOpType(args.Op),
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

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastApplied = 0
	kv.dead = 0
	kv.MemoryKVStateMachine = NewMemoryKVStateMachine()
	// You may need initialization code here.
	go kv.applyTicker()

	return kv
}

func (kv *KVServer) applyTicker() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				kv.mu.Lock()
				//已经处理就忽略
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				//取出用户操作
				op := message.Command.(Op)
				//操作应用到状态机
				OpReply := kv.applyToMemoryKVStateMachine(op)

				//将结果返回到应用
				if _, IsLeader := kv.rf.GetState(); IsLeader {
					notifyCh := kv.getNotifyChannel(message.CommandIndex)
					notifyCh <- OpReply
				}
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) applyToMemoryKVStateMachine(op Op) *OpReply {
	var value string
	var Err Err
	switch op.OpType {
	case OpGet:
		value, Err = kv.MemoryKVStateMachine.Get(op.Key)
	case OpPut:
		Err = kv.MemoryKVStateMachine.Put(op.Key, op.Value)
	case OpAppend:
		Err = kv.MemoryKVStateMachine.Append(op.Key, op.Value)
	}
	return &OpReply{Value: value, Err: Err}
}

func (kv *KVServer) getNotifyChannel(index int) chan *OpReply {
	if _, ok := kv.notifyCh[index]; !ok {
		kv.notifyCh[index] = make(chan *OpReply, 1)
	}
	return kv.notifyCh[index]
}

// 这个channel是唯一的(由日志的index决定，所以用完要释放)
func (kv *KVServer) removeNotifyChannel(index int) {
	delete(kv.notifyCh, index)
}
