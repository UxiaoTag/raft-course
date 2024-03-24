package main

import (
	"bufio"
	"course/shardctrler"
	"course/shardkv"
	"fmt"
	"os"
	"strconv"
)

func main() {
	cfg := shardkv.Makeconfig(3, true, 100)
	defer cfg.Cleanup()

	gis := []int{0, 1, 2}
	cfg.Joinm(gis)
	ck := cfg.MakeClient()
	fmt.Println("use for shardkv,Client ID:", ck.GetClientId())
	for {
		exit := false
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan() // 读取输入的一行
		text := scanner.Text()
		switch text {
		case "exit":
			exit = true
		case "get":
			fmt.Printf("get your key:")
			scanner.Scan()
			key := scanner.Text()
			value := ck.Get(key)
			fmt.Printf("Shard %dvalue:%s", key2shard(key), value)
		case "put":
			fmt.Printf("put your key:")
			scanner.Scan()
			key := scanner.Text()
			fmt.Printf("put your value:")
			scanner.Scan()
			value := scanner.Text()
			fmt.Printf("Key: %s, Value: %s\n", key, value) // 格式化输出
			ck.Put(key, value)
		case "append":
			fmt.Printf("append your key:")
			scanner.Scan()
			key := scanner.Text()
			fmt.Printf("append your value:")
			scanner.Scan()
			value := scanner.Text()
			fmt.Printf("Key: %s, Value: %s\n", key, value) // 格式化输出
			ck.Append(key, value)
		case "get Config":
			ctlclient := cfg.Getmck()
			config := ctlclient.Query(-1)
			gidToShard := MakegidToShards(config)
			for gid, shards := range gidToShard {
				fmt.Printf("group%d shards:", gid)
				for _, shard := range shards {
					fmt.Printf("%d ", shard)
				}
				fmt.Println("")
			}
		case "leave":
			fmt.Printf("leave group num:")
			scanner.Scan()
			str := scanner.Text()
			gid, err := strconv.Atoi(str)
			if err == nil && contains(gis, gid) {
				cfg.Leave(gid)
			} else {
				fmt.Printf("Err: %s or num out of gis,gid%d\n", err, gid)
			}
		case "":
			continue
		default:
			fmt.Println("unkown op!")
		}

		if exit {
			break
		}
	}
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

func contains(slice []int, val int) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}
