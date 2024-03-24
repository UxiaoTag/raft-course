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
	get := func(ctx *gin.Context) {
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
		nowConfig := cfg.Getmck().Query(-1)
		ctx.JSON(http.StatusOK, nowConfig)
	}

	router := gin.Default()

	router.GET("/Get", get)
	router.GET("/GetConfig", func(ctx *gin.Context) {
		nowConfig := cfg.Getmck().Query(-1)
		ctx.JSON(http.StatusOK, nowConfig)
	})
	router.POST("/PutOrAppend", PutOrAppenfunc)
	router.POST("/JoinOrLeave", JoinOrLeaveFunc)

	router.Run()
}
