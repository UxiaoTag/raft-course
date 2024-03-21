package shardkv

import (
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
						//这里有个究极疑问，就是如果你执行了10条命令并且都apply了，然后你apply了一条seq为1的命令，
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
