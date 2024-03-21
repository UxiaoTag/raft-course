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
				if command.CmdType == ClientOpertion {
					op := command.Data.(Op)

					//如果该命令不是get且返回过
					if op.OpType != OpGet && kv.requestDuplicated(op.ClientId, op.SeqId) {
						OpReply = kv.duplicateTable[op.ClientId].Reply
					} else {
						//操作应用到状态机
						//这里有个究极疑问，就是如果你在客户端1104执行了10条命令并且都apply了，然后你突然拿到需要apply一条seq为1的命令，
						//你肯定是返回kv.duplicateTable[op.clientId].Reply，但是此时你的kv.duplicateTable[op.clientId].seqId其实应该==10，
						//也就是你返回的是10的返回结果，虽然我理解这个最重要的是不执行，且返回估计就是ok，没什么区别但是还是不理解。
						OpReply = kv.applyToMemoryKVStateMachine(op)
						if op.OpType != OpGet {
							kv.duplicateTable[op.ClientId] = lastOperationInfo{
								SeqId: op.SeqId,
								Reply: OpReply,
							}
						}
					}
				} else {
					// config := command.Data.(shardctrler.Config)
					OpReply = kv.handleConfigChangeMessage(command)
				}

				//将结果返回到应用
				if _, IsLeader := kv.rf.GetState(); IsLeader {
					notifyCh := kv.getNotifyChannel(message.CommandIndex)
					notifyCh <- OpReply
				}

				//判断是否需要snapshot
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					kv.makeSnapShot(message.CommandIndex)
				}
				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				kv.restoreSnapShot(message.Snapshot)
				kv.lastApplied = message.SnapshotIndex
				kv.mu.Unlock()
			}
		}
	}
}

// TODO
func (kv *ShardKV) fetchConfigTicker() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			//每次下一份配置
			NewConfig := kv.mck.Query(kv.currentConfig.Num + 1)
			//传入raft模块进行同步TODO
			kv.ConfigCommand(RaftCommand{
				CmdType: ConfigChange,
				Data:    NewConfig,
			}, &OpReply{})
			kv.currentConfig = NewConfig
			kv.mu.Unlock()
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
							kv.ConfigCommand(RaftCommand{ShardMingration, getShardReply}, &OpReply{})
						}
					}
				}(kv.prevConfig.Groups[gid], kv.currentConfig.Num, shardIds)
			}
			kv.mu.Unlock()
		}
		time.Sleep(shardMigrationIntval)
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
		reply.ShardData[shardid] = kv.shards[shardid].copyData()
	}

	//拷贝去重表
	reply.duplicateTable = make(map[int64]lastOperationInfo)
	for clientId, op := range kv.duplicateTable {
		reply.duplicateTable[clientId] = op.copyData()
	}

	reply.ConfigNum, reply.Err = args.ConfigNum, OK
}
