package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"sync"
	"sync/atomic"
	"time"

	//	"course/labgob"
	"course/labrpc"
)

const (
	electionTimeoutMin time.Duration = 250 * time.Millisecond
	electionTimeoutMax time.Duration = 400 * time.Millisecond
	replicateInterval  time.Duration = 70 * time.Millisecond
)

type Role string

// 定义角色状态为常量
const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For PartD:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//PartA
	role        Role
	currentTerm int
	votedFor    int //在currentTerm任期是否投过票 -1 means none

	electionStart   time.Time     //选举起始点
	electionTimeout time.Duration //选举时间间隔

	//commit index and last applied

	commitIndex int //全局日志提交进度
	lastApplied int //本 Peer 日志 apply 进度
	applyCond   *sync.Cond
	applyCh     chan ApplyMsg //此处有疑问

	log []LogEntry //log in Peer's local

	//used in Leader
	//匹配点视图
	nextIndex  []int //for each server, index of the next log entryto send to that server (initialized to leaderlast log index + 1)
	matchIndex []int //for each server, index of highest log entryknown to be replicated on server(initialized to 0,increases monotonically)
}

func (rf *Raft) becomeFollowerLocked(term int) {
	if term < rf.currentTerm {
		//在此处，你执行了becomeFollower意味着你需要变成follower，你不应该被比你低的任期的所更改状态
		LOG(rf.me, rf.currentTerm, DError, "Lower TermT%d,sould no be Follower", term)
		return
	}

	LOG(rf.me, rf.currentTerm, DLog, "%s->Follower,term T%d->T%d", rf.role, rf.currentTerm, term)
	// rf.mu.Lock()
	rf.role = Follower
	// important! Could only reset the `votedFor` when term increased
	if term > rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = term
	// rf.mu.Unlock()
}

func (rf *Raft) becomeCandidateLocked() {
	if rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DVote, "you are Leader,not be Candidate")
		return
	}

	LOG(rf.me, rf.currentTerm, DLog, "%s->Candidate,term T%d->T%d", rf.role, rf.currentTerm, rf.currentTerm+1)
	// rf.mu.Lock()
	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
	// rf.mu.Unlock()
	rf.resetElectionTimerLocked()
}

func (rf *Raft) becomeLeaderLocked() {
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DVote, "just Candidate can be Leader")
		return
	}

	LOG(rf.me, rf.currentTerm, DLeader, "Become LeaderS%d,term T%d", rf.me, rf.currentTerm)
	// rf.mu.Lock()
	rf.role = Leader
	// rf.mu.Unlock()

	//成为leader之后需要维护matchIndex和nextIndex
	for peer := 0; peer < len(rf.peers); peer++ {
		//设定所有其他peer的下一个日志为leader的下一个日志
		rf.nextIndex[peer] = len(rf.log)
		//设定所有其他peer和leader还没匹配过日志
		rf.matchIndex[peer] = 0
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here (PartA).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (PartC).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (PartC).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock() //出现data race请先检查锁有没有上好
	// index := -1
	// term := -1
	// isLeader := true

	// Your code here (PartB).
	if rf.role != Leader {
		// isLeader = false
		return 0, 0, false
	}
	rf.log = append(rf.log, LogEntry{
		CommandValid: true,
		Command:      command,
		Term:         rf.currentTerm,
	})
	LOG(rf.me, rf.currentTerm, DLeader, "Leader accept log [%d]T%d", len(rf.log)-1, rf.currentTerm)

	return len(rf.log) - 1, rf.currentTerm, true //这里-1踩了一个坑，事实上你的index确实应该从0开始记(前面有一个空日志，直接算会从1记)
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 期间任期未改变，状态未变化返回false
func (rf *Raft) contextLostLocked(role Role, term int) bool {
	return !(rf.currentTerm == term && rf.role == role)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (PartA, PartB, PartC).
	//PartA
	rf.currentTerm = 0
	rf.role = Follower
	rf.votedFor = -1

	//PartB
	rf.log = append(rf.log, LogEntry{})

	//init leader view
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	//init used for apply
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.applicationTicker()

	return rf
}
