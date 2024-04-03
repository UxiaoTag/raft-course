package shardkv

import (
	minibitcask "course/mini-bitcask"
	"fmt"
	"os"
	"path/filepath"
)

type MemoryKVStateMachine struct {
	KV     *minibitcask.MiniBitcask
	Status ShardStatus
}

func NewMemoryKVStateMachine(shard, me, gid int) *MemoryKVStateMachine {
	// 获取当前工作目录的绝对路径
	currentDir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	pathStr := fmt.Sprintf("%d-%d-%d", gid, me, shard)
	targetDir := filepath.Join(currentDir, "tmp", "minibitcask"+pathStr)
	kv, err := minibitcask.Open(targetDir)
	if err != nil {
		panic(err)
	}
	return &MemoryKVStateMachine{
		KV:     kv,
		Status: Normal,
	}
}

func (mkv *MemoryKVStateMachine) Get(key string) (string, Err) {
	valueByte, err := mkv.KV.Get([]byte(key))
	if err == nil {
		return string(valueByte), OK
	}
	//无论出现任何错误，我们都返回NoKey
	return "", ErrNoKey
}

func (mkv *MemoryKVStateMachine) Put(key, value string) Err {
	mkv.KV.Put([]byte(key), []byte(value))
	return OK
}

func (mkv *MemoryKVStateMachine) Append(key, value string) Err {
	valueByte, _ := mkv.KV.Get([]byte(key))
	newValueByte := append(valueByte, []byte(value)...)
	mkv.KV.Put([]byte(key), newValueByte)
	return OK
}

func (mkv *MemoryKVStateMachine) copyData() map[string]string {
	newKV := make(map[string]string, 0)
	for _, key := range mkv.KV.GetKey() {
		valueByte, _ := mkv.KV.Get([]byte(key))
		newKV[key] = string(valueByte)
	}
	return newKV
}

func (mkv *MemoryKVStateMachine) GetSize() int {
	//这方法太慢了
	// return len(mkv.KV.GetKey())
	return mkv.KV.GetSize()
}
