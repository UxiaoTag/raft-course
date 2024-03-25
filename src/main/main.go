package main

import (
	"course/shardkv"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

func main() {

	//init servers
	cfg := shardkv.Makeconfig(5, true, 100)
	defer cfg.Cleanup()

	//join shardKVServer
	gis := []int{0, 1, 2}
	cfg.Joinm(gis)
	//make shardKVClient()
	ck := cfg.MakeClient()
	fmt.Println("use for shardkv,Client ID:", ck.GetClientId())

	//init Getfunc
	getFunc := func(ctx *gin.Context) {
		key := ctx.Query("key")
		if key == "" {
			ctx.JSON(http.StatusBadRequest, gin.H{"error": "Missing key parameter"})
		} else {
			value := ck.Get(key)
			ctx.JSON(http.StatusOK, gin.H{
				"key":   key,
				"value": value,
				"Shard": key2shard(key),
			})
		}
	}

	//init PutOrAppenfunc
	PutOrAppenfunc := func(ctx *gin.Context) {
		var data PostData
		if err := ctx.ShouldBindJSON(&data); err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		op := data.getOpType()
		if op != OpPut && op != OpAppend {
			println(data.Op)
			ctx.JSON(http.StatusBadRequest, gin.H{"error": "No Put or Append OP"})
			return
		}
		switch data.getOpType() {
		case OpPut:
			ck.Put(data.Key, data.Value)
		case OpAppend:
			ck.Append(data.Key, data.Value)
		}
		ctx.JSON(http.StatusOK, gin.H{
			"key":   data.Key,
			"value": data.Value,
			"Shard": key2shard(data.Key),
		})

	}

	//init JoinOrLeavefunc
	JoinOrLeaveFunc := func(ctx *gin.Context) {
		var data ConfigData
		if err := ctx.ShouldBindJSON(&data); err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		// 检查操作类型是否为OpJoin或OpLeave
		if data.getOpType() != OpJoin && data.getOpType() != OpLeave {
			ctx.JSON(http.StatusBadRequest, gin.H{"error": "Operation type must be JOIN or LEAVE"})
			return
		}
		switch data.getOpType() {
		case OpJoin:
			if !contains(gis, data.Num) {
				ctx.JSON(http.StatusBadRequest, gin.H{"error": "id not in group"})
				return
			}
			//获取控制台客户端
			// mck := cfg.Getmck()
			cfg.Joinm([]int{data.Num})
		case OpLeave:
			if !contains(gis, data.Num) {
				ctx.JSON(http.StatusBadRequest, gin.H{"error": "id not in group"})
				return
			}
			//获取控制台客户端
			// mck := cfg.Getmck()
			cfg.Leave(data.Num)
		}
		//执行完后返回当前配置：
		changeConfig := cfg.Getmck().Query(-1)
		ctx.JSON(http.StatusOK, changeConfig)
	}

	//init shutdownServer/startServerfunc
	StartOrShutdownFunc := func(ctx *gin.Context) {
		var data LifeData
		if err := ctx.ShouldBindJSON(&data); err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		// 检查操作类型是否为OpShudown或OpStart
		if data.getOpType() != OpShutdown && data.getOpType() != OpStart {
			ctx.JSON(http.StatusBadRequest, gin.H{"error": "Operation type must be Start or Shutdown"})
			return
		}
		switch data.getOpType() {
		case OpShutdown:
			cfg.ShutdownShardKvServer(data.Gid, data.Id)
		case OpStart:
			cfg.StartShardKvServer(data.Gid, data.Id)
		}
		ctx.JSON(http.StatusOK, data)
	}

	//init CheckNodefunc
	CheckNoedFunc := func(ctx *gin.Context) {
		var data LifeData
		if err := ctx.ShouldBindJSON(&data); err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		// 检查操作类型是否为OpCheckNode
		if data.getOpType() != OpCheckNode {
			ctx.JSON(http.StatusBadRequest, gin.H{"error": "Operation type must be OpCheckNode"})
			return
		}
		status := ck.CheckNode(data.Gid+100, data.Id)
		ctx.JSON(http.StatusOK, gin.H{
			"Gid":        data.Gid,
			"Id":         data.Id,
			"NodeStatus": status,
		})
	}

	router := gin.Default()

	router.GET("/Get", getFunc)
	router.GET("/GetConfig", func(ctx *gin.Context) {
		nowConfig := cfg.Getmck().Query(-1)
		ctx.JSON(http.StatusOK, nowConfig)
	})
	router.GET("/GetLeader", func(ctx *gin.Context) {
		Leaderids := ck.GetLeader()
		ctx.JSON(http.StatusOK, Leaderids)
	})
	router.GET("/MakegidToShards", func(ctx *gin.Context) {
		gidToShards := MakegidToShards(cfg.Getmck().Query(-1))
		ctx.JSON(http.StatusOK, gidToShards)
	})
	router.GET("/CheckNode", CheckNoedFunc)
	router.POST("/PutOrAppend", PutOrAppenfunc)
	router.POST("/JoinOrLeave", JoinOrLeaveFunc)
	//尽量不要使用该功能，关了请立刻开回去，不然坏机了
	router.POST("/StartOrShutdown", StartOrShutdownFunc)

	router.Run()
}

//cfg.StartShardKvServer(groupindex, id{0,1,2,3,4})
//cfg.ShutdownShardKvServer(groupindex, id{0,1,2,3,4})
