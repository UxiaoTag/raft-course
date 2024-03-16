package raft

import (
	"fmt"
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
	ConfilictIndex int
	ConfilictTerm  int
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Prev:[%d]T%d, (%d, %d], CommitIdx: %d",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm,
		args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
}
func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, Sucess: %v, ConfilictTerm: [%d]T%d", reply.Term, reply.Success, reply.ConfilictIndex, reply.ConfilictTerm)
}

// AppendEntries回调函数 RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Appended, Args=%v", args.LeaderId, args.String())
	reply.Term = rf.currentTerm
	reply.Success = false

	//如果任期低，应该让他放弃
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d,Lower Term,Reject log", args.LeaderId)
		return
	}

	//只要任期对齐之后，之后日志对不上导致心跳不正常，也要让选举时钟重置。保证提前执行的return也要执行选举时钟重置
	//任期高，主动让自己变回去
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
		// LOG(rf.me, rf.currentTerm, DLog2, "<- S%d,Append Heart", args.LeaderId) //加入日志之后还需要做日志的比对
	}
	//如果是日志原因拒绝心跳，就输出一些日志,并重置选举时钟
	defer func() {
		rf.resetElectionTimerLocked()
		if !reply.Success {
			LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Follower Conflict: [%d]T%d", args.LeaderId, reply.ConfilictIndex, reply.ConfilictTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "Follower log=%v", rf.log.String())
		}
	}()
	//最后的括号表示立即执行

	//这里理解了，大概是原log小于当试探点时，中间空缺的部分需要重新下放试探点，如果直接运行rf.log[args.PrevLogIndex]会越界。
	//这里俩段可以写在一起，但是保证调试看着舒服，就不用and直接连接逻辑了
	if args.PrevLogIndex >= rf.log.size() {
		//然后等待心跳逻辑重新发送更低的试探点
		//如果探测点大于当前，直接给当前日志长度
		reply.ConfilictIndex = rf.log.size()
		reply.ConfilictTerm = InvalidTerm
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d,Reject log,follower log too short, Len:%d<=Prev:%d", args.LeaderId, rf.log.size(), args.PrevLogIndex)
		// rf.resetElectionTimerLocked()
		return
	}

	//如果 Follower 的前面的日志与 Leader 发送的日志在相同的位置处的 Term 不一致，这可能表示存在日志冲突或者日志错误的情况。
	//在这种情况下，Follower 会拒绝接受 Leader 发送的日志，并打印相应的日志以及进行必要的处理
	//append日志需要保证append之前的日志是正确的
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		//然后等待心跳逻辑重新发送更低的试探点

		//这段逻辑应该也能用，这里是这个任期第一个日志-1
		// idx := args.PrevLogIndex
		// iTerm := rf.log[idx].Term
		// for idx > 0 && rf.log[idx].Term != iTerm {
		// 	idx--
		// }
		// reply.ConfilictIndex = idx
		// reply.ConfilictTerm = iTerm
		//如果不是，则找到日志错误任期的第一份日志的index进行比对
		reply.ConfilictTerm = rf.log.at(args.PrevLogIndex).Term
		reply.ConfilictIndex = rf.log.firstForTerm(reply.ConfilictTerm)
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d,Reject log,Prev log not match [%d]:T%d!=T%d", args.LeaderId, args.PrevLogIndex, rf.log.at(args.PrevLogIndex).Term, args.PrevLogTerm)
		return
	}

	//append log 这里选择用rf.log[:args.PrevLogIndex+1]因为直接使用len(rf.log)，本身rf.log可能比你传输的日志长，会导致节点不一致
	//eg L1:12344 F2:12345,探测点为3，传入日志应该为45，如果PrevLogIndex不+1，则F2:12445
	// rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	rf.log.appendFrom(args.PrevLogIndex, args.Entries)
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
	// rf.resetElectionTimerLocked()

}

// 传输心跳
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 获取日志的主要匹配值
func (rf *Raft) getmajorityIndexLocked() int {
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
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d,Lost send", peer)
			return
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Reply=%v", peer, reply.String())
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
			if reply.ConfilictTerm == InvalidTerm {
				//如果传入任期是无效的，直接用传的index
				rf.nextIndex[peer] = reply.ConfilictIndex
			} else {
				firstTermIndex := rf.log.firstForTerm(reply.ConfilictTerm)
				if firstTermIndex != InvalidIndex {
					//如果有效先试试leader在这个任期有没有log,用leader的index。
					rf.nextIndex[peer] = firstTermIndex
				} else {
					//不行再用follower的index
					rf.nextIndex[peer] = reply.ConfilictIndex
				}
			}
			// rf.nextIndex[peer] = min(prevNext, rf.nextIndex[peer])该特性用不了
			//如果优化的值甚至不如原来，没必要优化
			if rf.nextIndex[peer] > prevNext {
				rf.nextIndex[peer] = prevNext
			}
			nextprevIdx := rf.nextIndex[peer] - 1
			nextprevTerm := InvalidTerm
			//保证不会取到快照以下的term导致出错
			if nextprevIdx >= rf.log.snapLastIdx {
				nextprevTerm = rf.log.at(nextprevIdx).Term
			}
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Not matched at Prev=[%d]T%d, Try next Prev=[%d]T%d", peer, args.PrevLogIndex, args.PrevLogTerm, nextprevIdx, nextprevTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "Leader log=%v", rf.log.String())
			return
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

		}

		// reply.Success log appended, update match/next index
		//这里只能保证match同步上了当前函数的日志点，不能直接使用本地，如果函数执行期间本地的日志新增，matchlog就对不上
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		//update the commitIndex
		//主要匹配通过对匹配到的日志取中位数得到的进度进行应用
		majorityMatched := rf.getmajorityIndexLocked()
		if majorityMatched > rf.commitIndex && rf.log.at(majorityMatched).Term == rf.currentTerm {
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
			rf.matchIndex[peer] = rf.log.size() - 1
			rf.nextIndex[peer] = rf.log.size()
			continue
		}

		//获取peer中日志的试探点
		prevIdx := rf.nextIndex[peer] - 1
		// prevTerm := rf.log.at(prevIdx).Term //这里出现的读取panic: 1 is out of [10, 15]

		//如果目前匹配点小于快照点，让他把之前的快照做了
		if prevIdx < rf.log.snapLastIdx {
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.log.snapLastIdx,
				LastIncludedTerm:  rf.log.snapLastTerm,
				Snaphot:           rf.log.snapshot,
			}
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, InstallSnap, Args=%v", peer, args.String())
			go rf.installOnPeer(peer, term, args)
			continue
		}
		prevTerm := rf.log.at(prevIdx).Term //如果做快照就不读取prevTerm
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log.tail(prevIdx + 1),
			LeaderCommit: rf.commitIndex,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, %v", peer, args.String())
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
