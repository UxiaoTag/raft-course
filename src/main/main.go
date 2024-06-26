package main

import (
	"course/shardkv"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func main() {

	//init servers
	cfg := Makeconfig(4, true, 100)
	defer cfg.Cleanup()

	//join shardKVServer
	gis := []int{0, 1, 2}
	cfg.Joinm(gis)
	//make shardKVClient()
	ck := cfg.MakeClient()
	mck := cfg.Getmck()
	fmt.Println("use for shardkv,Client ID:", ck.GetClientId())

	//init Getfunc
	getFunc := func(ctx *gin.Context) {
		key := ctx.Query("key")
		if key == "" {
			ctx.JSON(http.StatusBadRequest, gin.H{"error": "Missing key parameter"})
		} else {
			value := ck.Get(key)
			if value == "ErrTimeout" {
				ctx.JSON(http.StatusRequestTimeout, gin.H{"error": "Get operation timed out"})
				return
			}
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
		var ok shardkv.Err
		switch data.getOpType() {
		case OpPut:
			ok = ck.Put(data.Key, data.Value)
		case OpAppend:
			ok = ck.Append(data.Key, data.Value)
		}
		if ok == shardkv.ErrTimeout {
			ctx.JSON(http.StatusRequestTimeout, gin.H{"error": "PutOrAppend operation timed out"})
			return
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
			cfg.Join(data.Num)
		case OpLeave:
			if !contains(gis, data.Num) {
				ctx.JSON(http.StatusBadRequest, gin.H{"error": "id not in group"})
				return
			}
			cfg.Leave(data.Num)
		}
		//执行完后返回当前配置：
		changeConfig := mck.Query(-1)
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
		config := mck.Query(-1)
		if servers, ok := config.Groups[data.Gid]; ok && len(servers) > 0 && data.Id >= 0 && data.Id < len(servers) {
			switch data.getOpType() {
			case OpShutdown:
				cfg.ShutdownShardKvServer(data.Gid-100, data.Id)
			case OpStart:
				cfg.StartShardKvServer(data.Gid-100, data.Id)
			}
			ctx.JSON(http.StatusOK, data)
			return
		}
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Gid Or Id Error"})
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

	//init CheckAllNodefunc
	CheckAllNoedFunc := func(ctx *gin.Context) {
		// 初始化StatusRequest
		statusMap := make(map[int]map[int]bool)
		// 获取配置
		config := mck.Query(-1)
		// 遍历配置中的所有组
		for gid, servers := range config.Groups {
			// 初始化每个组的节点状态
			statusMap[gid] = make(map[int]bool)
			// 遍历组中的所有服务器
			for index, _ := range servers {
				// 检查节点状态
				err := ck.CheckNode(gid, index)
				if err == shardkv.ErrTimeout {
					// 节点状态超时，标记为false
					statusMap[gid][index] = false
				} else {
					// 如果节点返回OK，找不到key，不是Leader，我们都认为节点存活标记为true，有些不应该发生的情况比如errgroup之类的
					statusMap[gid][index] = true

				}
			}
		}
		ctx.JSON(http.StatusOK, statusMap)
	}

	//init getlogs
	getLogsFunc := func(ctx *gin.Context) {
		logFilePath := "alter_raft.log" // 你的日志文件路径

		// 定义一个用于接收JSON数据的结构体变量
		var request LogRequest

		// 绑定JSON请求体到结构体中
		if err := ctx.ShouldBindJSON(&request); err != nil {
			ctx.Error(err).SetMeta("Error binding JSON request body")
			ctx.AbortWithStatus(http.StatusBadRequest)
			return
		}

		// 读取日志文件的最后部分
		result, err := tailLog(logFilePath)
		if err != nil {
			ctx.Error(err).SetMeta("Error reading log file")
			ctx.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		// 如果lastLineContent为空，或者匹配不上，则返回全部日志
		logList := result
		if request.LastLineContent != "" {
			// 查找lastLineContent在result中的索引
			for i, line := range result {
				if strings.TrimSpace(line) == strings.TrimSpace(request.LastLineContent) {
					// 如果找到lastLineContent，则只返回该行之后的日志
					logList = result[i+1:]
					break
				}
			}
		}

		// 设置响应数据
		ctx.JSON(http.StatusOK, gin.H{
			"log_list":     logList,      // 新的日志行列表
			"current_line": len(logList), // 当前读取到的行数
		})
	}

	router := gin.Default()

	// 启用 CORS
	config := cors.DefaultConfig()
	config.AllowAllOrigins = true          // 允许所有源
	config.AddAllowHeaders("content-type") // 允许请求头

	router.Use(cors.New(config))

	router.GET("/Get", getFunc)
	router.GET("/GetConfig", func(ctx *gin.Context) {
		nowConfig := mck.Query(-1)
		ctx.JSON(http.StatusOK, nowConfig)
	})
	router.GET("/GetLeader", func(ctx *gin.Context) {
		Leaderids := ck.GetLeader()
		ctx.JSON(http.StatusOK, Leaderids)
	})
	router.GET("/MakegidToShards", func(ctx *gin.Context) {
		gidToShards := MakegidToShards(mck.Query(-1))
		ctx.JSON(http.StatusOK, gidToShards)
	})
	router.GET("/CheckNode", CheckNoedFunc)
	router.GET("/CheckAllNode", CheckAllNoedFunc)
	router.GET("/GetAll", func(ctx *gin.Context) {
		shardstr := ctx.Query("shard")
		shard, err := strconv.Atoi(shardstr)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		ShardKV := ck.GetAll(shard)
		ctx.JSON(http.StatusOK, ShardKV)
	})
	router.GET("/GetSize", func(ctx *gin.Context) {
		shardstr := ctx.Query("shard")
		shard, err := strconv.Atoi(shardstr)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		lenShard := ck.GetSize(shard)
		ctx.JSON(http.StatusOK, gin.H{
			"shard": shardstr,
			"len":   lenShard,
		})
	})
	router.GET("/Bug", func(ctx *gin.Context) {
		cfg.Cleanup()

		//获取工作目录
		currentDir, err := os.Getwd()
		if err != nil {
			panic("read Dir Error:" + err.Error() + "\n")
		}
		// 定义tmp目录的路径
		tmpDir := filepath.Join(currentDir, "tmp")
		//清空
		err = os.RemoveAll(tmpDir)
		if err != nil {
			panic("Error removing tmp directory:" + err.Error() + "\n")
		}
	})
	router.POST("/GetLogs", getLogsFunc)
	router.POST("/PutOrAppend", PutOrAppenfunc)
	router.POST("/JoinOrLeave", JoinOrLeaveFunc)
	//尽量不要使用该功能，关了请立刻开回去，不然坏机了
	router.POST("/StartOrShutdown", StartOrShutdownFunc)

	router.Run()
}

//cfg.StartShardKvServer(groupindex, id{0,1,2,3,4})
//cfg.ShutdownShardKvServer(groupindex, id{0,1,2,3,4})
