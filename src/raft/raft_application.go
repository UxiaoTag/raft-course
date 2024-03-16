package raft

func (rf *Raft) applicationTicker() {
	for !rf.killed() {

		//这里套锁也不理解，大概意思是mu.Lock锁和applyCond.Wait开，applyCond.applyCond.Signal()锁和mu.Unlock开
		rf.mu.Lock()
		rf.applyCond.Wait()
		//当其他函数通过applyCond调用当前Ticker时,将执行同步日志传入
		entries := make([]LogEntry, 0)
		snapPendingInstall := rf.snapPending
		if !snapPendingInstall {
			//如果当前日志提交进度小于快照进度，让进度直接飞到快照进度
			if rf.lastApplied < rf.log.snapLastIdx {
				rf.lastApplied = rf.log.snapLastIdx
			}
			start := rf.lastApplied + 1
			end := rf.commitIndex
			if end > rf.log.size()-1 {
				end = rf.log.size() - 1
			}
			for i := start; i <= end; i++ {
				entries = append(entries, rf.log.at(i))
			}
		}
		rf.mu.Unlock()

		if !snapPendingInstall {
			//这段不理解，大概是遍历这些 msgs，进行 apply，这里日志的应用是增量式的，故需要for读取所有apply然后进行
			for i, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: entry.CommandValid,
					Command:      entry.Command,
					CommandIndex: rf.lastApplied + 1 + i, //必须严谨

				}
			}
		} else {
			//但是这里是覆盖式的
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.log.snapshot,
				SnapshotIndex: rf.log.snapLastIdx,
				SnapshotTerm:  rf.log.snapLastTerm,
			}
		}

		//这里是应用成功后，修改本地apply进度
		rf.mu.Lock()
		if !snapPendingInstall {
			LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
			rf.lastApplied += len(entries)
		} else {
			LOG(rf.me, rf.currentTerm, DApply, "Install Snapshot for [0, %d]", rf.log.snapLastIdx)
			rf.lastApplied = rf.log.snapLastIdx
			if rf.commitIndex < rf.lastApplied {
				rf.commitIndex = rf.lastApplied
			}
			rf.snapPending = false
		}

		rf.mu.Unlock()
	}
}
