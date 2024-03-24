package main

import (
	"course/shardctrler"
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

//leave和join直接使用cfg中的函数，可以直接使用了。

//use for shutdown/start
//谨慎使用
//cfg.StartServer(gi,i) gi是指启动的时候的group的index,[]int{0,1,2}里面的0/1/2都可以
//cfg.ShutdownServer(gi, i)也是同理

//use test
// for {
// 	exit := false
// 	scanner := bufio.NewScanner(os.Stdin)
// 	scanner.Scan() // 读取输入的一行
// 	text := scanner.Text()
// 	switch text {
// 	case "exit":
// 		exit = true
// 	case "get":
// 		fmt.Printf("get your key:")
// 		scanner.Scan()
// 		key := scanner.Text()
// 		value := ck.Get(key)
// 		fmt.Printf("Shard %dvalue:%s", key2shard(key), value)
// 	case "put":
// 		fmt.Printf("put your key:")
// 		scanner.Scan()
// 		key := scanner.Text()
// 		fmt.Printf("put your value:")
// 		scanner.Scan()
// 		value := scanner.Text()
// 		fmt.Printf("Key: %s, Value: %s\n", key, value) // 格式化输出
// 		ck.Put(key, value)
// 	case "append":
// 		fmt.Printf("append your key:")
// 		scanner.Scan()
// 		key := scanner.Text()
// 		fmt.Printf("append your value:")
// 		scanner.Scan()
// 		value := scanner.Text()
// 		fmt.Printf("Key: %s, Value: %s\n", key, value) // 格式化输出
// 		ck.Append(key, value)
// 	case "get Config":
// 		ctlclient := cfg.Getmck()
// 		config := ctlclient.Query(-1)
// 		gidToShard := MakegidToShards(config)
// 		for gid, shards := range gidToShard {
// 			fmt.Printf("group%d shards:", gid)
// 			for _, shard := range shards {
// 				fmt.Printf("%d ", shard)
// 			}
// 			fmt.Println("")
// 		}
// 	case "leave":
// 		fmt.Printf("leave group num:")
// 		scanner.Scan()
// 		str := scanner.Text()
// 		gid, err := strconv.Atoi(str)
// 		if err == nil && contains(gis, gid) {
// 			cfg.Leave(gid)
// 		} else {
// 			fmt.Printf("Err: %s or num out of gis,gid%d\n", err, gid)
// 		}
// 	case "":
// 		continue
// 	default:
// 		fmt.Println("unkown op!")
// 	}

// 	if exit {
// 		break
// 	}
// }
