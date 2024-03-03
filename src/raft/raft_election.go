package raft

import (
	//	"bytes"
	"math/rand"
	"time"
	//	"course/labgob"
)

func (rf *Raft) resetElectionTimerLocked() {
	rf.electionStart = time.Now()
	randge := int64(electionTimeoutMax - electionTimeoutMin)
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63n(randge))
}

func (rf *Raft) isElecationTimeoutLocked() bool {
	//Since计算当前时间与start差值
	return time.Since(rf.electionStart) > rf.electionTimeout
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	//发送包含任期以及发送者id
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	//返回包含任期以及是否同意
	Term         int
	VotedGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (PartA, PartB).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	// if rf.contextLostLocked(rf.role, rf.currentTerm) {
	// 	LOG(rf.me, rf.currentTerm, DVote, "收到的发送状态异常")
	// }
	//传入任期小于自己
	if rf.currentTerm > args.Term {
		LOG(rf.me, rf.currentTerm, DVote, "Lower Term,Reject S%d,T%d>T%d", args.CandidateId, args.Term, rf.currentTerm)
		reply.VotedGranted = false
		return
	}
	//大于任期则自动变
	if rf.currentTerm < args.Term {
		rf.becomeFollowerLocked(args.Term)
	}

	//投过票了，(becomeFollower比自己大的任期会清空选票)
	if rf.vortedFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "Reject S%d,vortedFor S%d", args.CandidateId, rf.vortedFor)
		reply.VotedGranted = false
		return
	}

	reply.VotedGranted = true
	rf.vortedFor = args.CandidateId
	rf.resetElectionTimerLocked()
	LOG(rf.me, rf.currentTerm, DVote, "vortedFor ->S%d", args.CandidateId)

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

func (rf *Raft) startElecation(term int) bool {
	votes := 0
	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(peer, args, reply)

		//上锁，并查看是是否答应
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "Ask vote from S%d,lost or error", peer)
			return
		}

		//如果自己的任期小于返回任期
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		//检查状态是否变化
		if rf.contextLostLocked(rf.role, rf.currentTerm) {
			LOG(rf.me, rf.currentTerm, DVote, "Status Lower,stop ElecationS%d", peer)
			return
		}

		//确定发送没有问题，返回值为愿意投票
		if reply.VotedGranted {
			votes++
			if votes > len(rf.peers)/2 {
				rf.becomeLeaderLocked()
				//同步其他日志，宣示主权和心跳
				go rf.replicationTicker(term)
			}
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "期间状态发生变化,终止投票S%s", rf.role)
		return false
	}
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			votes++
			continue
		}
		args := &RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
		}
		//askVoteFromPeer是指构造 RPC 参数、发送 RPC等待结果、对 RPC 结果进行处理,写成函数写在上面了
		go askVoteFromPeer(peer, args)
	}

	return true
}

func (rf *Raft) electionTicker() {
	for !rf.killed() {

		// Your code here (PartA)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role != Leader && rf.isElecationTimeoutLocked() {
			rf.becomeCandidateLocked()
			go rf.startElecation(rf.currentTerm)
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
