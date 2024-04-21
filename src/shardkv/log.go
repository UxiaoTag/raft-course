package shardkv

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	DInfo  logTopic = "INFO" // level = 1
	DDebug logTopic = "DBUG" // level = 0
)

func getTopicLevel(topic logTopic) int {
	switch topic {
	case DInfo:
		return 1
	case DDebug:
		return 0
	default:
		return 1
	}
}

func getEnvLevel() int {
	// v := os.Getenv("VERBOSE")
	v := "0" //默认debug级，让他把都输出
	level := getTopicLevel(DInfo) + 1
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var logStart time.Time
var logLevel int
var logFile *os.File

func init() {
	var err error
	logLevel = getEnvLevel()
	logStart = time.Now()

	// do not print verbose date
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))

	//将日志持久化到本地，方便查看
	logFile, err = os.OpenFile("alter_raft.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic("Create Log File Error:" + err.Error())
	}
	log.SetOutput(logFile)
}

func LOG(gId int, peerId int, topic logTopic, format string, a ...interface{}) {
	topicLevel := getTopicLevel(topic)
	if logLevel <= topicLevel {
		time := time.Since(logStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d G%04d %v S%d ", time, gId, string(topic), peerId)
		format = prefix + format
		log.Printf(format, a...)
	}
}
