package shardctrler

import (
	"course/labgob"
	"course/labrpc"
	"course/raft"
	"sync"
	"sync/atomic"
	"time"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	dead                 int32 // set by Kill()
	lastApplied          int
	MemoryKVStateMachine *MemoryKVStateMachine
	notifyCh             map[int]chan *OpReply

	duplicateTable map[int64]lastOperationInfo
}

// 如果重复发送过返回true,这里seqId如果<=存储中的info.seqId，说明这条命令是在之前就执行过的。
func (sc *ShardCtrler) requestDuplicated(clientId, seqId int64) bool {
	info, ok := sc.duplicateTable[clientId]
	return ok && seqId <= info.SeqId
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	var OpReply OpReply
	sc.Command(Op{
		OpType:   OpJoin,
		ClientId: args.ClientId,
		SeqId:    args.ClientId,
		Servers:  args.Servers,
	}, &OpReply)
	reply.Err = OpReply.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	var OpReply OpReply
	sc.Command(Op{
		OpType:   OpLeave,
		ClientId: args.ClientId,
		SeqId:    args.ClientId,
		GIDs:     args.GIDs,
	}, &OpReply)
	reply.Err = OpReply.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	var OpReply OpReply
	sc.Command(Op{
		OpType:   OpMove,
		ClientId: args.ClientId,
		SeqId:    args.ClientId,
		Shard:    args.Shard,
		GID:      args.GID,
	}, &OpReply)
	reply.Err = OpReply.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	var OpReply OpReply
	sc.Command(Op{
		OpType: OpQuery,
		Num:    args.Num,
	}, &OpReply)
	reply.Err = OpReply.Err
	reply.Config = OpReply.ControlerConfig
}

func (sc *ShardCtrler) Command(args Op, reply *OpReply) {

	//查重
	sc.mu.Lock()
	if args.OpType != OpQuery && sc.requestDuplicated(args.ClientId, args.SeqId) {
		reply.Err = sc.duplicateTable[args.ClientId].Reply.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	index, _, isLeader := sc.rf.Start(args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	sc.mu.Lock()
	notifych := sc.getNotifyChannel(index)
	sc.mu.Unlock()

	select {
	case result := <-notifych:
		reply.Err = result.Err
		reply.ControlerConfig = result.ControlerConfig
	case <-time.After(ClientRequsetTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		sc.removeNotifyChannel(index)
		sc.mu.Unlock()
	}()

}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	sc.lastApplied = 0
	sc.dead = 0
	sc.MemoryKVStateMachine = NewMemoryKVStateMachine()
	sc.notifyCh = make(map[int]chan *OpReply)
	sc.duplicateTable = make(map[int64]lastOperationInfo)

	// You may need initialization code here.
	go sc.applyTicker()

	return sc
}

func (sc *ShardCtrler) applyTicker() {
	for !sc.killed() {
		select {
		case message := <-sc.applyCh:
			if message.CommandValid {
				sc.mu.Lock()
				//已经处理就忽略
				if message.CommandIndex <= sc.lastApplied {
					sc.mu.Unlock()
					continue
				}
				sc.lastApplied = message.CommandIndex

				//取出用户操作
				op := message.Command.(Op)

				var OpReply *OpReply
				//如果该命令不是Query且返回过
				if op.OpType != OpQuery && sc.requestDuplicated(op.ClientId, op.SeqId) {
					OpReply = sc.duplicateTable[op.ClientId].Reply
				} else {
					//操作应用到状态机
					//这里有个究极疑问，就是如果你执行了10条命令并且都apply了，然后你apply了一条seq为1的命令，
					//你肯定是返回kv.duplicateTable[op.clientId].Reply，但是此时你的kv.duplicateTable[op.clientId].seqId其实应该==10，
					//也就是你返回的是10的返回结果，虽然我理解这个最重要的是不执行，且返回估计就是ok，没什么区别但是还是不理解。
					OpReply = sc.applyToMemoryKVStateMachine(op)
					if op.OpType != OpQuery {
						sc.duplicateTable[op.ClientId] = lastOperationInfo{
							SeqId: op.SeqId,
							Reply: OpReply,
						}
					}
				}
				//将结果返回到应用
				if _, IsLeader := sc.rf.GetState(); IsLeader {
					notifyCh := sc.getNotifyChannel(message.CommandIndex)
					notifyCh <- OpReply
				}

				sc.mu.Unlock()
			}
		}
	}
}

// TODO
func (sc *ShardCtrler) applyToMemoryKVStateMachine(op Op) *OpReply {
	// var value string
	// var Err Err
	// switch op.OpType {
	// case OpGet:
	// 	value, Err = sc.MemoryKVStateMachine.Get(op.Key)
	// case OpPut:
	// 	Err = sc.MemoryKVStateMachine.Put(op.Key, op.Value)
	// case OpAppend:
	// 	Err = sc.MemoryKVStateMachine.Append(op.Key, op.Value)
	// }
	return nil
}

func (sc *ShardCtrler) getNotifyChannel(index int) chan *OpReply {
	if _, ok := sc.notifyCh[index]; !ok {
		sc.notifyCh[index] = make(chan *OpReply, 1)
	}
	return sc.notifyCh[index]
}

// 这个channel是唯一的(由日志的index决定，所以用完要释放)
func (sc *ShardCtrler) removeNotifyChannel(index int) {
	delete(sc.notifyCh, index)
}
