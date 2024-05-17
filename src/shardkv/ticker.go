package shardkv

import (
	"sync"
	"time"
)

// apply log apply To KVStateMachine And return client
func (kv *ShardKV) applyTicker() {
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
				command := message.Command.(RaftCommand)
				var OpReply *OpReply
				if command.CmdType == ClientOperation {
					op := command.Data.(Op)
					OpReply = kv.applyClientOp(op)
					LOG(kv.gid, kv.me, DInfo, "Apply "+opTypeString(op.OpType)+" Operation")
				} else {
					// config := command.Data.(shardctrler.Config)
					OpReply = kv.handleConfigChangeMessage(command)
					LOG(kv.gid, kv.me, DInfo, "Apply "+raftOpTypeString(command.CmdType)+" Operation")
				}

				//将结果返回到应用
				if _, IsLeader := kv.rf.GetState(); IsLeader {
					notifyCh := kv.getNotifyChannel(message.CommandIndex)
					notifyCh <- OpReply
				}

				//判断是否需要snapshot
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					//这里倒是出过一个的问题，panic: 57 is out of [57, 56]
					// println("%d>=%d,sould be snapshot", kv.rf.GetRaftStateSize(), kv.maxraftstate)
					LOG(kv.gid, kv.me, DDebug, "Shard KV Start SnapShot Group:%d Peer:%d ,LogNum:%d", kv.gid, kv.me, kv.rf.GetRaftStateSize())
					kv.makeSnapShot(message.CommandIndex)
					// //做完SnapShot顺便Merge
					// for shardid := range kv.shards {
					// 	id := shardid
					// 	go func() {
					// 		kv.shards[id].KV.Merge()
					// 	}()
					// }
				}
				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				kv.restoreSnapShot(message.Snapshot)
				kv.lastApplied = message.SnapshotIndex
				LOG(kv.gid, kv.me, DDebug, "Shard KV Restore SnapShot Group:%d Peer:%d ,LogNum:%d", kv.gid, kv.me, kv.rf.GetRaftStateSize())
				kv.mu.Unlock()
			}
		}
	}
}

// 获取当前配置
func (kv *ShardKV) fetchConfigTicker() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {

			needFetch := true
			kv.mu.Lock()
			//每次处理一份请求,如果有shard状态是非normal,则说明配置还在进行变更
			for _, shard := range kv.shards {
				if shard.Status != Normal {
					needFetch = false
					break
				}
			}
			currentNum := kv.currentConfig.Num
			kv.mu.Unlock()

			if needFetch {
				NewConfig := kv.mck.Query(currentNum + 1)
				if NewConfig.Num == currentNum+1 {
					//传入raft模块进行同步,如果configNUM一致则不需要修改
					kv.ConfigCommand(RaftCommand{
						CmdType: ConfigChange,
						Data:    NewConfig,
					}, &OpReply{})
					//当新建配置，这里先发一个Insert
					LOG(kv.gid, kv.me, DInfo, "Insert "+raftOpTypeString(ConfigChange)+" operation")
				}
			}
			time.Sleep(FetchConfigIntval)
		}
	}
}

func (kv *ShardKV) shardMigrationTicker() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			//找到需要进来的shard
			gidToShard := kv.getShardByStatus(MoveIn)
			var wg sync.WaitGroup
			for gid, shardIds := range gidToShard {
				wg.Add(1)
				go func(servers []string, ConfigNum int, shardIds []int) {
					defer wg.Done()
					//遍历gourp中的每一个节点，然后从leader当中读取对应的shard数据
					getShardArgs := ShardOperationArgs{ConfigNum: ConfigNum, ShardIds: shardIds}
					for _, server := range servers {
						var getShardReply ShardOperationReply
						clientEnd := kv.make_end(server)
						ok := clientEnd.Call("ShardKV.GetShardData", &getShardArgs, &getShardReply)
						//数据拿到了
						if ok && getShardReply.Err == OK {
							//用log传递给所有人
							kv.ConfigCommand(RaftCommand{ShardMigration, getShardReply}, &OpReply{})
							//当拿到数据，插入日志时，这里发一个Insert标识数据迁入
							LOG(kv.gid, kv.me, DInfo, "Insert "+raftOpTypeString(ShardMigration)+" operation")
						}
					}
				}(kv.prevConfig.Groups[gid], kv.currentConfig.Num, shardIds)
			}
			kv.mu.Unlock()
			wg.Wait()
		}
		time.Sleep(shardMigrationIntval)
	}
}

func (kv *ShardKV) shardGCTicker() {
	for !kv.killed() {
		if _, isleader := kv.rf.GetState(); isleader {
			kv.mu.Lock()
			gidToShards := kv.getShardByStatus(GC)
			var wg sync.WaitGroup
			for gid, shardIds := range gidToShards {
				wg.Add(1)
				go func(servers []string, ConfigNum int, shardIds []int) {
					defer wg.Done()
					shardArgs := ShardOperationArgs{ConfigNum, shardIds}
					for _, server := range servers {
						var shardGCReply ShardOperationReply
						clientEnd := kv.make_end(server)
						ok := clientEnd.Call("ShardKV.DeleteShardData", &shardArgs, &shardGCReply)
						if ok && shardGCReply.Err == OK {
							//这里发送的意思是当你发送给对方MOVEOUT的可以GC了，并收到OK，则自己修改GC状态
							kv.ConfigCommand(RaftCommand{ShardGC, shardArgs}, &OpReply{})
							//当发送给别人删除分片数据成功之后，自己清空GC状态为Nomal
							LOG(kv.gid, kv.me, DInfo, "Insert "+raftOpTypeString(ShardGC)+" operation GC->Normal")
						}
					}
				}(kv.prevConfig.Groups[gid], kv.currentConfig.Num, shardIds)
			}
			kv.mu.Unlock()
			wg.Wait()
		}
		time.Sleep(shardGCIntval)
	}
}

func (kv *ShardKV) getShardByStatus(status ShardStatus) map[int][]int {
	gidToShard := make(map[int][]int, 0)
	for i, shard := range kv.shards {
		if shard.Status == status {
			gid := kv.prevConfig.Shards[i]
			if gid != 0 {
				if _, ok := gidToShard[gid]; !ok {
					gidToShard[gid] = make([]int, 0)
				}
				gidToShard[gid] = append(gidToShard[gid], i)
			}
		}
	}
	return gidToShard
}

func (kv *ShardKV) GetShardData(args *ShardOperationArgs, reply *ShardOperationReply) {
	//只从leader中拿取
	if _, isleader := kv.rf.GetState(); !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//当前Group的配置不是所需要的
	if kv.currentConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReadly
		return
	}

	//拷贝数据
	reply.ShardData = make(map[int]map[string]string)
	for _, shardid := range args.ShardIds {
		//先Sync一下防止拷贝出错
		// kv.shards[shardid].KV.Sync()
		reply.ShardData[shardid] = kv.shards[shardid].copyData()
	}

	//拷贝去重表
	reply.DuplicateTable = make(map[int64]lastOperationInfo)
	for clientId, op := range kv.duplicateTable {
		reply.DuplicateTable[clientId] = op.copyData()
	}

	reply.ConfigNum, reply.Err = args.ConfigNum, OK
}

func (kv *ShardKV) DeleteShardData(args *ShardOperationArgs, reply *ShardOperationReply) {
	if _, isleader := kv.rf.GetState(); !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()

	//当前Group的配置不是所需要的
	if kv.currentConfig.Num > args.ConfigNum {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	var opReply OpReply
	//下达GC，在applyShardGC中实现
	kv.ConfigCommand(RaftCommand{ShardGC, *args}, &opReply)
	//当发送给别人删除分片数据成功之后，这里主要就是MoveOut将其清空
	LOG(kv.gid, kv.me, DInfo, "Insert "+raftOpTypeString(ShardGC)+" operation MoveOut->Normal")
	reply.Err = opReply.Err
}

func (kv *ShardKV) applyClientOp(op Op) *OpReply {
	if kv.matchGroup(op.Key) {
		var Reply *OpReply
		//如果该命令不是get且返回过
		if op.OpType != OpGet && kv.requestDuplicated(op.ClientId, op.SeqId) {
			Reply = kv.duplicateTable[op.ClientId].Reply
			// println("me:", kv.rf.GetMe(), "No push client:", op.ClientId, ",sendSeqId", op.SeqId, " kv.duplicateTable[op.ClientId].nowSeqId", kv.duplicateTable[op.ClientId].SeqId, " ,opType", op.OpType, " ,opkeyvlaue", op.Key, op.Value)
		} else {
			//操作应用到状态机
			//这里有个究极疑问，就是如果你在客户端1104执行了10条命令并且都apply了，然后你突然拿到需要apply一条seq为1的命令，
			//你肯定是返回kv.duplicateTable[op.clientId].Reply，但是此时你的kv.duplicateTable[op.clientId].seqId其实应该==10，
			//也就是你返回的是10的返回结果，虽然我理解这个最重要的是不执行，且返回估计就是ok，没什么区别但是还是不理解。
			Reply = kv.applyToMemoryKVStateMachine(op)
			if op.OpType != OpGet && op.OpType != OpGetAll && op.OpType != OpGetSize {
				// println("me:", kv.rf.GetMe(), "push client:", op.ClientId, ",sendSeqId", op.SeqId, " kv.duplicateTable[op.ClientId].nowSeqId", kv.duplicateTable[op.ClientId].SeqId, " ,opType", op.OpType, " ,opkeyvlaue", op.Key, op.Value)
				kv.duplicateTable[op.ClientId] = lastOperationInfo{
					SeqId: op.SeqId,
					Reply: Reply,
				}
			}
		}
		return Reply
	}
	return &OpReply{Err: ErrWrongGroup}
}
