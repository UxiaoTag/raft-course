package shardkv

import (
	"course/shardctrler"
	"time"
)

func (kv *ShardKV) ConfigCommand(command RaftCommand, reply *OpReply) {
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 是leader就记录发送日志
	LOG(kv.gid, kv.me, DInfo, "Insert "+raftOpTypeString(command.CmdType)+" operation")
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

// 处理配置变更消息
func (kv *ShardKV) handleConfigChangeMessage(command RaftCommand) *OpReply {
	switch command.CmdType {
	case ConfigChange:
		config := command.Data.(shardctrler.Config)
		return kv.applyNewConfig(config)
	case ShardMigration:
		shardData := command.Data.(ShardOperationReply)
		return kv.applyShardMingration(&shardData)
	case ShardGC:
		shardInfo := command.Data.(ShardOperationArgs)
		return kv.applyShardGC(&shardInfo)
	default:
		panic("unknow config change type")
	}
}

// apply Change Config
func (kv *ShardKV) applyNewConfig(newConfig shardctrler.Config) *OpReply {
	if kv.currentConfig.Num+1 == newConfig.Num {
		for i := 0; i < shardctrler.NShards; i++ {
			//处理分段进来
			if kv.currentConfig.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid {
				// 需要迁移进来
				gid := kv.currentConfig.Shards[i]
				if gid != 0 {
					kv.shards[i].Status = MoveIn
				}
			}
			//处理分段离开
			if kv.currentConfig.Shards[i] == kv.gid && newConfig.Shards[i] != kv.gid {
				//需要迁移出去
				gid := kv.currentConfig.Shards[i]
				if gid != 0 {
					kv.shards[i].Status = MoveOut
				}
			}

		}
		kv.prevConfig = kv.currentConfig
		kv.currentConfig = newConfig
		LOG(kv.gid, kv.me, DDebug, "DB apply New Config %d", newConfig.Num)
		return &OpReply{Err: OK}
	}
	return &OpReply{Err: ErrWrongConfig}
}

func (kv *ShardKV) applyShardMingration(reply *ShardOperationReply) *OpReply {
	if reply.ConfigNum == kv.currentConfig.Num {
		for shardId, shardData := range reply.ShardData {
			shard := kv.shards[shardId]
			//将数据存储到当前grop对应的shard中
			if shard.Status == MoveIn {
				// kv.shards[shardId].KV = shardData
				for k, v := range shardData {
					// shard.KV[k] = v
					shard.KV.Put([]byte(k), []byte(v))
				}
				//执行完毕之后Sync一下
				shard.KV.Sync()
				shard.Status = GC
				LOG(kv.gid, kv.me, DDebug, "DB MoveIn -> GC ,Shrad:%d", shardId)
			} else {
				break
			}
		}

		//拷贝去重表的数据
		for clientId, table := range reply.DuplicateTable {
			localtable, ok := kv.duplicateTable[clientId]
			if !ok || localtable.SeqId < table.SeqId {
				kv.duplicateTable[clientId] = table
			}
		}
		return &OpReply{Err: OK}
	}
	return &OpReply{Err: ErrWrongConfig}
}

func (kv *ShardKV) applyShardGC(shardInfo *ShardOperationArgs) *OpReply {
	if shardInfo.ConfigNum == kv.currentConfig.Num {
		for _, shardId := range shardInfo.ShardIds {
			shard := kv.shards[shardId]
			if shard.Status == GC {
				shard.Status = Normal
				LOG(kv.gid, kv.me, DDebug, "DB GC -> Normal  ,Shrad:%d", shardId)
			} else if shard.Status == MoveOut {
				//clear shard，这里会Close掉文件。
				kv.shards[shardId].KV.ClearAll()
				//clear完毕之后从新使用NewMemoryKVStateMachine，会通过open重新创建一个数据库实例，因为文件也删了，会重建数据库
				kv.shards[shardId] = NewMemoryKVStateMachine(shardId, kv.me, kv.gid)
				shard.Status = Normal
				LOG(kv.gid, kv.me, DDebug, "DB MoveOut -> Normal  ,Shrad:%d", shardId)
			} else {
				break
			}
		}
	}
	return &OpReply{Err: OK}
}
