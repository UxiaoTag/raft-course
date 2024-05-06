package main

import (
	"bufio"
	"bytes"
	"course/shardctrler"
	"io"
	"os"
)

type OpType uint8

const NShards = 10

const (
	OpGet OpType = iota
	OpPut
	OpAppend
	OpQuery
	OpJoin
	OpLeave
	OpStart
	OpShutdown
	OpCheckNode
)

// use for put/Append
type PostData struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
	Op    string `json:"Op"`
}

// use for join/leave
type ConfigData struct {
	Num int    `json:"Num"`
	Op  string `json:"Op"`
}

// use for start/shutdown/checknode
type LifeData struct {
	Gid int    `json:"Gid"`
	Id  int    `json:"Id"`
	Op  string `json:"Op"` // 操作类型标识
}

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func MakegidToShards(config shardctrler.Config) map[int][]int {
	gidToShards := make(map[int][]int)

	if len(config.Groups) == 0 {
		return nil
	}
	for gid := range config.Groups {
		gidToShards[gid] = make([]int, 0)
	}

	for shard, gid := range config.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}
	return gidToShards
}

// 对比数组内是否含有某一变量
func contains(slice []int, val int) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

// client.Get(key)
// client.Put(key, value)
// client.Append(key, vlaue)
// get GetLeader 还需要确定gid=group index还是group 内容
// 当然这里的leader拿的是客户端的leader，所以如果更新需要一次成功的该分组的，client操作

func (data *PostData) getOpType() OpType {
	var op OpType
	switch data.Op {
	case "Get":
		op = OpGet
	case "Put":
		op = OpPut
	case "Append":
		op = OpAppend
	// case "Query":
	// 	op = OpQuery
	// case "Join":
	// 	op = OpJoin
	default:
		panic("unkown op,It sould be in here")
	}
	return op
}

func (data *ConfigData) getOpType() OpType {
	var op OpType
	switch data.Op {
	// case "Get":
	// 	op = OpGet
	// case "Put":
	// 	op = OpPut
	case "Append":
		op = OpAppend
	case "Query":
		op = OpQuery
	case "Join":
		op = OpJoin
	case "Leave":
		op = OpLeave
	default:
		panic("unkown op,It sould be in here")
	}
	return op
}

func (data *LifeData) getOpType() OpType {
	var op OpType
	switch data.Op {
	case "Start":
		op = OpStart
	case "Shutdown":
		op = OpShutdown
	case "CheckNode":
		op = OpCheckNode
	default:
		panic("unkown op,It sould be in here")
	}
	return op
}

//leave和join直接使用cfg中的函数，可以直接使用了。

//use for shutdown/start
//谨慎使用
//cfg.StartServer(gi,i) gi是指启动的时候的group的index,[]int{0,1,2}里面的0/1/2都可以
//cfg.ShutdownServer(gi, i)也是同理

// tailLog 函数用于读取文件的最后几行
func tailLog(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// 获取文件信息
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	size := fileInfo.Size()

	// 如果文件大小为0，直接返回空的字符串切片
	if size == 0 {
		return nil, nil
	}

	// 定义一个足够大的缓冲区以读取文件的最后一部分
	const bufferSize = 1024 * 10 // 可以调整这个值以适应你的需要
	buffer := make([]byte, bufferSize)

	// 定位到文件末尾前bufferSize字节的位置，如果文件大小小于bufferSize，则从头开始读取
	startPos := size
	if size <= int64(bufferSize) {
		startPos = 0
	} else {
		startPos = size - int64(bufferSize)
	}

	_, err = file.Seek(startPos, io.SeekStart)
	if err != nil {
		return nil, err
	}

	// 读取最后一部分数据
	readSize, err := io.ReadFull(file, buffer)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return nil, err
	}

	// 初始化结果切片
	var result []string
	scanner := bufio.NewScanner(bytes.NewReader(buffer[:readSize]))
	for scanner.Scan() {
		// 将读取到的行追加到结果切片中，并保持行的原始顺序
		result = append(result, scanner.Text())
	}

	// 如果遇到扫描错误，返回错误
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// LogRequest 结构体用于绑定POST请求的JSON数据
type LogRequest struct {
	LastLineContent string `json:"lastLineContent" binding:"omitempty"`
}
