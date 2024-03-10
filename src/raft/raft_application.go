package raft

func (rf *Raft) applicationTicker() {
	for !rf.killed() {

		//这里套锁也不理解，大概意思是mu.Lock锁和applyCond.Wait开，applyCond.applyCond.Signal()锁和mu.Unlock开
		rf.mu.Lock()
		rf.applyCond.Wait()
		//当其他函数通过applyCond调用当前Ticker时,将执行同步日志传入
		entries := make([]LogEntry, 0)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			entries = append(entries, rf.log[i])
		}

		rf.mu.Unlock()

		//这段不理解，大概是遍历这些 msgs，进行 apply
		for i, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: entry.CommandValid,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied + 1 + i, //必须严谨
			}
		}

		//这里是应用成功后，修改本地apply进度
		rf.mu.Lock()
		LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
		rf.lastApplied += len(entries)
		rf.mu.Unlock()
	}
}
