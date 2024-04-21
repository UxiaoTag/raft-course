package shardkv

import "testing"

func TestLog(t *testing.T) {
	LOG(1, 2, DInfo, "TESTLOG,use for shardkv")
	LOG(1, 2, DInfo, raftOpTypeString(ClientOperation))
	LOG(1, 2, DInfo, opTypeString(OpGet))
}
