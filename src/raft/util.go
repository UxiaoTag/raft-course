package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	DError logTopic = "ERRO" // level = 3
	DWarn  logTopic = "WARN" // level = 2
	DInfo  logTopic = "INFO" // level = 1
	DDebug logTopic = "DBUG" // level = 0

	// level = 1
	DClient  logTopic = "CLNT"
	DCommit  logTopic = "CMIT"
	DDrop    logTopic = "DROP"
	DLeader  logTopic = "LEAD"
	DLog     logTopic = "LOG1" // sending log
	DLog2    logTopic = "LOG2" // receiving log
	DPersist logTopic = "PERS"
	DSnap    logTopic = "SNAP"
	DTerm    logTopic = "TERM"
	DTest    logTopic = "TEST"
	DTimer   logTopic = "TIMR"
	DTrace   logTopic = "TRCE"
	DVote    logTopic = "VOTE"
	DApply   logTopic = "APLY"
)

func getTopicLevel(topic logTopic) int {
	switch topic {
	case DError:
		return 3
	case DWarn:
		return 2
	case DInfo:
		return 1
	case DDebug:
		return 0
	default:
		return 1
	}
}

func getEnvLevel() int {
	v := os.Getenv("VERBOSE")
	level := getTopicLevel(DError) + 1
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

func LOG(peerId int, term int, topic logTopic, format string, a ...interface{}) {
	topicLevel := getTopicLevel(topic)
	if logLevel <= topicLevel {
		time := time.Since(logStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d T%04d %v S%d ", time, term, string(topic), peerId)
		format = prefix + format
		log.Printf(format, a...)
	}
}
