package main

import (
	"course/utils"
	"fmt"
	"testing"
)

func TestLog_Test(t *testing.T) {

	// path, err := os.Getwd()
	// if err != nil {
	// 	panic(err)
	// }
	// path = filepath.Join(path, "tmp", "alter_raft.log")
	logFilePath := "alter_raft.log"
	result, err := tailLog(logFilePath)
	if err != nil {
		panic(err)
	}
	for _, line := range result {
		fmt.Println(line)
	}
	t.Fail()
}

func TestRandPut(t *testing.T) {
	//init servers
	cfg := Makeconfig(5, true, 100)
	defer cfg.Cleanup()

	//join shardKVServer
	gis := []int{0, 1, 2}
	cfg.Joinm(gis)
	//make shardKVClient()
	ck := cfg.MakeClient()
	fmt.Println("use for shardkv,Client ID:", ck.GetClientId())

	for i := 0; i < 10000; i++ {
		ck.Put(utils.GetTestKey(i), utils.RandomValue(10))
		fmt.Printf("Put key:%s ,in shard %d\n", utils.GetTestKey(i), key2shard(utils.GetTestKey(i)))
	}
	for i := 0; i < 10000; i++ {
		vlaue := ck.Get(utils.GetTestKey(i))
		t.Log(vlaue)
	}
	t.Fail()
}
