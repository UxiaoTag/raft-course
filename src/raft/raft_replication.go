package raft

import (
	"sort"
	"time"
)

// 定义日志的结构
type LogEntry struct {
	Term         int         //the log
	CommandValid bool        //if applied is true
	Command      interface{} //the log
}

// 定义发送或取消心跳rpc
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	//用于匹配点的试探
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry

	//used to update follower
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	//这里由于 Leader 在向 Follower 同步日志每次回撤都要等多个周期，回撤如果写的很烂，匹配探测期就会很长，所以这里定义follower自己也将日志
	//告诉leader,加快回撤效率
	ConflictIndex int
	ConflictTerm  int
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

	//只要任期对齐之后，之后日志对不上导致心跳不正常，也要让选举时钟重置。保证提前执行的return也要执行选举时钟重置
	defer rf.resetElectionTimerLocked()
	//任期高，主动让自己变回去
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
		// LOG(rf.me, rf.currentTerm, DLog2, "<- S%d,Append Heart", args.LeaderId) //加入日志之后还需要做日志的比对
	}

	//这里理解了，大概是原log小于当试探点时，中间空缺的部分需要重新下放试探点，如果直接运行rf.log[args.PrevLogIndex]会越界。
	//这里俩段可以写在一起，但是保证调试看着舒服，就不用and直接连接逻辑了
	if args.PrevLogIndex >= len(rf.log) {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d,Reject log,follower log too short, Len:%d<=Prev:%d", args.LeaderId, len(rf.log), args.PrevLogIndex)
		//然后等待心跳逻辑重新发送更低的试探点
		//用这个加速回撤
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = InvalidTerm
		rf.resetElectionTimerLocked()
		return
	}

	//如果 Follower 的前面的日志与 Leader 发送的日志在相同的位置处的 Term 不一致，这可能表示存在日志冲突或者日志错误的情况。
	//在这种情况下，Follower 会拒绝接受 Leader 发送的日志，并打印相应的日志以及进行必要的处理
	//append日志需要保证append之前的日志是正确的
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d,Reject log,Prev log not match [%d]:T%d!=T%d", args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		//然后等待心跳逻辑重新发送更低的试探点

		//这段逻辑应该也能用，这里是这个任期第一个日志-1
		// idx := args.PrevLogIndex
		// iTerm := rf.log[idx].Term
		// for idx > 0 && rf.log[idx].Term != iTerm {
		// 	idx--
		// }
		// reply.ConflictIndex = idx
		// reply.ConflictTerm = iTerm
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		reply.ConflictIndex = rf.firstLogFor(reply.ConflictTerm)
		return
	}

	//append log 这里选择用rf.log[:args.PrevLogIndex+1]因为直接使用len(rf.log)，本身rf.log可能比你传输的日志长，会导致节点不一致
	//eg L1:12344 F2:12345,探测点为3，传入日志应该为45，如果PrevLogIndex不+1，则F2:12445
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	//此处log改变，需要持久化
	rf.persistLocked()
	LOG(rf.me, rf.currentTerm, DLog2, "<- S%d,Follower append log,(%d,%d]", args.LeaderId, args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))
	reply.Success = true

	//hanle LeaderCommit

	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower Update The commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Signal()
	}

	//清空选举时钟
	rf.resetElectionTimerLocked()

}

// 传输心跳
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 获取日志的主要匹配值
func (rf *Raft) getmajorityMatchedLocked() int {
	tmpIndex := make([]int, len(rf.peers))
	copy(tmpIndex, rf.matchIndex)
	//此处有疑问sort.Ints(tmpIndex)不能直接用吗
	sort.Ints(sort.IntSlice(tmpIndex))
	//一般都是奇数个点，实话说感觉应该不太影响，-1也就只是日志修改判断比较保守
	majorityIdx := (len(rf.peers) - 1) / 2
	LOG(rf.me, rf.currentTerm, DDebug, "Match index after sort: %v, majority[%d]=%d", tmpIndex, majorityIdx, tmpIndex[majorityIdx])
	return tmpIndex[majorityIdx]
}

// 心跳启动函数
func (rf *Raft) startReplication(term int) bool {

	//单个rpc发送心跳函数replicateToPeer
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "->S%d,Lost send", peer)
			return
		}
		// align term
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}
		// check context lost
		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Context Lost, T%d:Leader->T%d:%s", peer, term, rf.currentTerm, rf.role)
			return
		}
		//说明你收到的是拒绝心跳,你可能需要下探你的底线
		if !reply.Success {
			prevNext := rf.nextIndex[peer]
			if reply.ConflictTerm == InvalidTerm {
				//如果传入任期是无效的，直接用传的index
				rf.nextIndex[peer] = reply.ConflictIndex
			} else {
				firstTermIndex := rf.firstLogFor(reply.ConflictTerm)
				if firstTermIndex != InvalidIndex {
					//如果有效先试试leader在这个任期有没有log,用leader的index。
					rf.nextIndex[peer] = firstTermIndex
				} else {
					//不行再用follower的index
					rf.nextIndex[peer] = reply.ConflictIndex
				}
			}
			// rf.nextIndex[peer] = min(prevNext, rf.nextIndex[peer])该特性用不了
			//如果优化的值甚至不如原来，没必要优化
			if rf.nextIndex[peer] > prevNext {
				rf.nextIndex[peer] = prevNext
			}

			//优化前
			// // 考虑过rf.nextIndex[peer]=args.PrevLogIndex--，但是一个个点试探太慢了
			// idx := rf.nextIndex[peer] - 1
			// term := rf.log[idx].Term
			// //寻找上一个任期的最新日志进行
			// for idx > 0 && rf.log[idx].Term == term {
			// 	idx--
			// }
			// //这里的逻辑时每次间隔一个任期进行日志获取，每次下探一个任期。
			// rf.nextIndex[peer] = idx + 1
			LOG(rf.me, rf.currentTerm, DLog, "->S%d, Log not matched in %d, Update next=%d", peer, args.PrevLogIndex, rf.nextIndex[peer])
			return
		}

		// reply.Success log appended, update match/next index
		//这里只能保证match同步上了当前函数的日志点，不能直接使用本地，如果函数执行期间本地的日志新增，matchlog就对不上
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		//update the commitIndex
		//主要匹配通过对匹配到的日志取中位数得到的进度进行应用
		majorityMatched := rf.getmajorityMatchedLocked()
		if majorityMatched > rf.commitIndex {
			LOG(rf.me, rf.currentTerm, DApply, "Leader update the commit index %d->%d", rf.commitIndex, majorityMatched)
			rf.commitIndex = majorityMatched
			rf.applyCond.Signal()
		}

	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLeader, "Leader[T%d]->%s[T%d],Context Lost", term, rf.role, rf.currentTerm)
		return false
	}
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			// Don't forget to update Leader's matchIndex
			rf.matchIndex[peer] = len(rf.log) - 1
			rf.nextIndex[peer] = len(rf.log)
			continue
		}

		//获取peer中日志的试探点
		prevIdx := rf.nextIndex[peer] - 1
		prevTerm := rf.log[prevIdx].Term

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log[prevIdx+1:],
			LeaderCommit: rf.commitIndex,
		}
		go replicateToPeer(peer, args)
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
