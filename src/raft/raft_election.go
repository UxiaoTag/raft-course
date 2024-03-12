package raft

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"time"
	//	"course/labgob"
)

func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("Candidate-%d T%d, Last:[%d]T%d", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
}

func (reply *RequestVoteReply) String() string {
	return fmt.Sprintf("T%d, VoteGranted: %v", reply.Term, reply.VoteGranted)
}

func (rf *Raft) resetElectionTimerLocked() {
	rf.electionStart = time.Now()
	randge := int64(electionTimeoutMax - electionTimeoutMin)
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63n(randge))
}

func (rf *Raft) isElecationTimeoutLocked() bool {
	//Since计算当前时间与start差值
	return time.Since(rf.electionStart) > rf.electionTimeout
}

// ture 则代表自己的日志更大
func (rf *Raft) isMoreUpToDateLocked(candidateIndex int, candidateTerm int) bool {
	// l := rf.log.size()
	lastIndex, lastTerm := rf.log.last() //l-1,rf.log.at(l-1).Term

	LOG(rf.me, rf.currentTerm, DVote, "Compare last log, Me: [%d]T%d, Candidate: [%d]T%d", lastIndex, lastTerm, candidateIndex, candidateTerm)
	if lastTerm != candidateTerm {
		return candidateTerm < lastTerm
	}
	return candidateIndex < lastIndex
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	//发送包含任期以及发送者id
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	//返回包含任期以及是否同意
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (PartA, PartB).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, VoteAsked, Args=%v", args.CandidateId, args.String())
	reply.Term = rf.currentTerm
	// if rf.contextLostLocked(rf.role, rf.currentTerm) {
	// 	LOG(rf.me, rf.currentTerm, DVote, "收到的发送状态异常")
	// }
	//传入任期小于自己
	reply.VoteGranted = false
	if rf.currentTerm > args.Term {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject voted, Higher term, T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
		// reply.VoteGranted = false
		return
	}
	//大于任期则自动变
	if rf.currentTerm < args.Term {
		rf.becomeFollowerLocked(args.Term)
	}

	//投过票了，(becomeFollower比自己大的任期会清空选票),完善一下逻辑
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject voted, Already voted to S%d", args.CandidateId, rf.votedFor)
		// reply.VoteGranted = false
		return
	}

	// check log, only grante vote when the candidates have more up-to-date log
	if rf.isMoreUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject voted, Candidate less up-to-date", args.CandidateId)
		return
	}
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	//投票改了故需要修改
	rf.persistLocked()
	rf.resetElectionTimerLocked()
	LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Vote granted", args.CandidateId)

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection(term int) {
	votes := 0
	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(peer, args, reply)

		//上锁，并查看是是否答应
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Ask vote, Lost or error", peer)
			return
		}
		LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, VoteAsked, Args=%v", args.CandidateId, args.String())
		//如果自己的任期小于返回任期
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		//检查状态是否变化
		if rf.contextLostLocked(Candidate, term) {
			LOG(rf.me, rf.currentTerm, DVote, "Status Lower,stop ElecationS%d", peer)
			return
		}

		//确定发送没有问题，返回值为愿意投票
		if reply.VoteGranted {
			votes++
			if votes > len(rf.peers)/2 {
				rf.becomeLeaderLocked()
				//同步其他日志，宣示主权和心跳
				go rf.replicationTicker(term)
			}
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock() //检查应该早于l取值
	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Context Lost,stop ElecationS%s", rf.role)
		return
	}
	//此处使用l而不是直接使用len(rf.log)是防止一些边界判断检查
	// l := rf.log.size()
	lastIdx, lastTerm := rf.log.last()
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			votes++
			continue
		}
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastIdx,
			LastLogTerm:  lastTerm,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote, Args=%v", peer, args.String())
		//askVoteFromPeer是指构造 RPC 参数、发送 RPC等待结果、对 RPC 结果进行处理,写成函数写在上面了
		go askVoteFromPeer(peer, args)
	}

}

func (rf *Raft) electionTicker() {
	for !rf.killed() {

		// Your code here (PartA)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role != Leader && rf.isElecationTimeoutLocked() {
			rf.becomeCandidateLocked()
			go rf.startElection(rf.currentTerm)
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
