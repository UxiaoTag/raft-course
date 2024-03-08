package raft

import "time"

// 定义发送或取消心跳rpc
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	//用于匹配点的试探
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries回调函数 RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d,Receive log,Prev=[%d]T%d,len()=%d", args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))

	reply.Term = rf.currentTerm
	reply.Success = false

	//如果任期低，应该让他放弃
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d,Lower Term,Reject log", args.LeaderId)
		return
	}

	//任期高，主动让自己变回去
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
		// LOG(rf.me, rf.currentTerm, DLog2, "<- S%d,Append Heart", args.LeaderId) //加入日志之后还需要做日志的比对
	}

	//这里理解了，大概是当试探点已经大于等于原log时，中间空缺的部分需要重新下放试探点，如果直接运行rf.log[args.PrevLogIndex]会越界。
	//这里俩段可以写在一起，但是保证调试看着舒服，就不用and直接连接逻辑了
	if args.PrevLogIndex >= len(rf.log) {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d,Reject log,follower log too short, Len:%d<=Prev:%d", args.LeaderId, len(rf.log), args.PrevLogIndex)
		//然后等待心跳逻辑重新发送更低的试探点
		rf.resetElectionTimerLocked()
		return
	}

	//如果 Follower 的前面的日志与 Leader 发送的日志在相同的位置处的 Term 不一致，这可能表示存在日志冲突或者日志错误的情况。
	//在这种情况下，Follower 会拒绝接受 Leader 发送的日志，并打印相应的日志以及进行必要的处理
	//append日志需要保证append之前的日志是正确的
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d,Reject log,Perv log not match [%d]:T%d!=T%d", args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		//然后等待心跳逻辑重新发送更低的试探点
		rf.resetElectionTimerLocked()
		return
	}

	//append log 这里选择用rf.log[:args.PrevLogIndex+1]因为直接使用len(rf.log)，本身rf.log可能比你传输的日志长，会导致节点不一致
	//eg L1:12344 F2:12345,探测点为3，传入日志应该为45，如果PrevLogIndex不+1，则F2:12445
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	LOG(rf.me, rf.currentTerm, DLog2, "<- S%d,Follower append log,(%d,%d]", args.LeaderId, args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))
	reply.Success = true

	//TODO:hanle LeaderCommit

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

	//单个rpc发送心跳函数
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
		//说明你收到的是拒绝心跳,你可能需要下探你的底线
		if !reply.Success {
			// 考虑过rf.nextIndex[peer]=args.PrevLogIndex--，但是一个个点试探太慢了
			idx := rf.nextIndex[peer] - 1
			term := rf.log[idx].Term
			//寻找上一个任期的最新日志进行
			for idx > 0 && rf.log[idx].Term == term {
				idx--
			}
			//这里的逻辑时每次间隔一个任期进行日志获取，每次下探一个任期。
			rf.nextIndex[peer] = idx + 1
			LOG(rf.me, rf.currentTerm, DLog, "Log not matched in %d, Update next=%d", args.PrevLogIndex, rf.nextIndex[peer])
			return
		}

		// reply.Success log appended, update match/next index
		//这里只能保证match同步上了当前函数的日志点，不能直接使用本地，如果函数执行期间本地的日志新增，matchlog就对不上
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		//TODO: update the commitIndex

	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLeader, "Leader[T%d]->%s[T%d],Lost Leader", term, rf.role, rf.currentTerm)
		return false
	}
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			// Don't forget to update Leader's matchIndex
			//这里再becomeLeader已经更新过了，但是在这里再更新一次
			rf.matchIndex[peer] = len(rf.log) - 1
			rf.nextIndex[peer] = len(rf.log)
			continue
		}

		//获取peer中日志的试探点
		pervIdx := rf.nextIndex[peer] - 1
		pervTerm := rf.log[pervIdx].Term

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: pervIdx,
			PrevLogTerm:  pervTerm,
			Entries:      rf.log[pervIdx+1:],
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
