package raft

import "time"

// 定义发送或取消心跳rpc
type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries回调函数 RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	//如果任期低，应该让他放弃
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d,Lower Term,Reject log", args.LeaderId)
		reply.Success = false
	}

	//任期高，主动让自己变回去
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d,Append Heart", args.LeaderId)
		reply.Success = true
	}

	//清空选举时钟
	rf.resetElectionTimerLocked()

}

// 传输心跳
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 心跳启动函数
func (rf *Raft) startReplication(term int) bool {

	//单个rpc传输函数
	replicationToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "->S%d,Lost send", peer)
			return
		}
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLeader, "Leader[T%d]->%s[T%d],Lost Leader", term, rf.role, rf.currentTerm)
		return false
	}
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}

		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		go replicationToPeer(peer, args)
	}
	return true
}

// 心跳计时器
func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {

		// Your code here (PartA)
		// Check if a leader election should be started.
		ok := rf.startReplication(term)
		if !ok {
			break
		}
		time.Sleep(replicateInterval)
	}
}
