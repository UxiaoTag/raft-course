package raft

import "testing"

func TestLOG(t *testing.T) {
	LOG(1, 1, DApply, "This is a TestLog")
}
