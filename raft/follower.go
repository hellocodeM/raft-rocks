package raft

import "time"

func (rf *Raft) doFollower() {
	rf.logInfo("Become follower")
	defer rf.logInfo("Follower quit")

	for {
		select {
		case <-rf.termChangedCh:
			return
		case <-rf.shutdownCh:
			return
		case <-time.After(electionTO()):
			rf.Lock()
			rf.role = candidate
			rf.votedFor = -1
			rf.persist()
			rf.Unlock()
			rf.logInfo("Follower lose heartbeat, become candidate")
			return
		case s := <-rf.appendEntriesCh:
			rf.processAppendEntries(s)
		case s := <-rf.requestVoteChan:
			rf.processRequestVote(s)
		case <-rf.snapshotCh:
			panic("not implemented")
		}
	}
}
