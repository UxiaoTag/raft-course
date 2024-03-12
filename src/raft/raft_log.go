package raft

import (
	"course/labgob"
	"fmt"
)

type RaftLog struct {
	snapLastIdx  int
	snapLastTerm int

	// contains index [1, snapLastIdx]
	snapshot []byte

	// the first entry is `snapLastIdx`, but only contains the snapLastTerm
	// the entries between (snapLastIdx, snapLastIdx+len(tailLog)-1] have real data
	tailLog []LogEntry
}

func NewLog(snapshotlastIndex int, snapshotlastTerm int, snapshot []byte, entries []LogEntry) *RaftLog {
	rl := &RaftLog{
		snapLastIdx:  snapshotlastIndex,
		snapLastTerm: snapshotlastTerm,
		snapshot:     snapshot,
	}
	rl.tailLog = make([]LogEntry, 0, 1+len(entries))
	rl.tailLog = append(rl.tailLog, LogEntry{
		Term: snapshotlastTerm,
	})
	rl.tailLog = append(rl.tailLog, entries...)
	return rl
}

// sould be lock
func (rl *RaftLog) readPersist(d *labgob.LabDecoder) error {
	var snapLastIdx int
	var snapLastTerm int
	// var snapshot []byte
	var log []LogEntry
	if err := d.Decode(&snapLastIdx); err != nil {
		return fmt.Errorf("decode last include index failed")
	}
	rl.snapLastIdx = snapLastIdx
	if err := d.Decode(&snapLastTerm); err != nil {
		return fmt.Errorf("decode last include term failed")
	}
	rl.snapLastTerm = snapLastTerm
	// if err := d.Decode(&snapshot); err != nil {
	// 	return fmt.Errorf("decode last include snapshot failed")
	// }
	// rl.snapshot = snapshot
	if err := d.Decode(&log); err != nil {
		return fmt.Errorf("decode tail log failed")
	}
	rl.tailLog = log
	return nil
}

// sould be lock
func (rl *RaftLog) Persist(e *labgob.LabEncoder) {
	e.Encode(rl.snapLastIdx)
	e.Encode(rl.snapLastTerm)
	// e.Encode(rl.snapshot)
	e.Encode(rl.tailLog)
}

// get max
func (rl *RaftLog) size() int {
	//最大限度为截断下标+截断后总长
	return rl.snapLastIdx + len(rl.tailLog)
}

// 下标转换，到了截断点，就当截断点是0
// index convertion
func (rl *RaftLog) idx(logicIdx int) int {
	if logicIdx < rl.snapLastIdx || logicIdx >= rl.size() {
		panic(fmt.Sprintf("%d is out of [%d, %d]", logicIdx, rl.snapLastIdx+1, rl.size()-1))
	}
	return logicIdx - rl.snapLastIdx
}

// 获取日志
func (rl *RaftLog) at(logicIdx int) LogEntry {
	return rl.tailLog[rl.idx(logicIdx)]
}

// 顺序寻找该任期最小的日志index
func (rl *RaftLog) firstForTerm(Term int) int {
	for i, entry := range rl.tailLog {
		if entry.Term == Term {
			return rl.snapLastIdx + i
		} else if entry.Term > Term {
			break
		}
	}
	return InvalidIndex
}

// 但是情况不能理解是，如果没记错的话，rf.log[0]是一个空日志，输出应该固定有一个是[0,0(1-1)]Term 0
// longstring将会输出每个任期的日志index持续段，例如[0,3]T1;[4,6]T2
// use for debug
func (rl *RaftLog) String() string {
	var terms string
	prevTerm := rl.at(0).Term
	prevStart := 0
	for i := 0; i < rl.size(); i++ {
		if rl.at(i).Term != prevTerm {
			terms += fmt.Sprintf(" [%d, %d]T%d;", prevStart, rl.snapLastIdx+i-1, prevTerm)
			prevTerm = rl.at(i).Term
			prevStart = i
		}
	}
	terms += fmt.Sprintf(" [%d, %d]T%d;", prevStart, rl.size()-1, prevTerm)
	return terms
}

// more simplified
func (rl *RaftLog) Str() string {
	lastIdx, lastTerm := rl.last()
	return fmt.Sprintf("[%d]T%d~[%d]T%d", rl.snapLastIdx, rl.snapLastTerm, lastIdx, lastTerm)
}

// append LogEntry
func (rl *RaftLog) append(entry LogEntry) {
	rl.tailLog = append(rl.tailLog, entry)
}

// append []LogEntry
func (rl *RaftLog) appendFrom(prevIdx int, entries []LogEntry) {
	rl.tailLog = append(rl.tailLog[:rl.idx(prevIdx)+1], entries...)
}

func (rl *RaftLog) last() (idx int, term int) {
	return rl.size() - 1, rl.tailLog[len(rl.tailLog)-1].Term
	//估计可以用rl.tailLog[rl.size()-rl.snapLastIdx-1].Term
}

func (rl *RaftLog) tail(startIdx int) []LogEntry {
	if startIdx >= rl.size() {
		return nil
	}
	return rl.tailLog[rl.idx(startIdx):]
}

func (rl *RaftLog) doSnapshot(index int, snapshot []byte) {
	idx := rl.idx(index)
	rl.snapLastTerm = rl.tailLog[idx].Term
	//这里记录的是传入的index
	rl.snapLastIdx = index
	rl.snapshot = snapshot

	//make new log
	newlog := make([]LogEntry, 0, rl.size()-rl.snapLastIdx)
	newlog = append(newlog, LogEntry{
		Term: rl.snapLastTerm,
	})
	newlog = append(newlog, rl.tailLog[idx+1:]...)
	rl.tailLog = newlog
}
