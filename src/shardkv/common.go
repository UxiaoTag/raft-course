package shardkv

import (
	"course/shardctrler"
	"fmt"
	"log"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrWrongConfig = "ErrWrongConfig"
	ErrNotReadly   = "ErrNotReadly"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	SeqId    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

const (
	ClientRequsetTimeout = 500 * time.Millisecond
	FetchConfigIntval    = 100 * time.Millisecond
	shardMigrationIntval = 50 * time.Millisecond
	shardGCIntval        = 50 * time.Millisecond
)
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	OpType   OpType
	ClientId int64
	SeqId    int64
}

type OpReply struct {
	Err   Err
	Value string
}

type OpType uint8

const (
	OpGet    OpType = 0
	OpPut    OpType = 1
	OpAppend OpType = 2
)

func getOpType(str string) OpType {
	switch str {
	case "Get":
		return OpGet
	case "Put":
		return OpPut
	case "Append":
		return OpAppend
	default:
		panic(fmt.Sprintf("unknow op"))
	}

}

type lastOperationInfo struct {
	SeqId int64
	Reply *OpReply
}

func (op *lastOperationInfo) copyData() lastOperationInfo {
	return lastOperationInfo{
		SeqId: op.SeqId,
		Reply: &OpReply{
			Err:   op.Reply.Err,
			Value: op.Reply.Value,
		},
	}
}

// 这里改造一下，将raft log请求分成配置变更和用户操作
type RaftOpType uint8

const (
	ClientOpertion RaftOpType = iota
	ConfigChange
	ShardMingration
	ShardGC
)

type RaftCommand struct {
	CmdType RaftOpType
	Data    interface{}
}

type ShardStatus uint8

const (
	Normal ShardStatus = iota
	MoveIn
	MoveOut
	GC
)

type ShardOperationArgs struct {
	ConfigNum int
	ShardIds  []int
}

type ShardOperationReply struct {
	Err            Err
	ConfigNum      int
	ShardData      map[int]map[string]string
	duplicateTable map[int64]lastOperationInfo
}

func (kv *ShardKV) matchGroup(Key string) bool {
	shard := key2shard(Key)
	return kv.currentConfig.Shards[shard] == kv.gid
}

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}
