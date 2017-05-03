package raft

import (
	"time"

	"github.com/golang/glog"
)

func (rf *Raft) doFollower() {
	glog.Infof("%s Become follower", rf.stateString())
	defer glog.Infof("%s Follower quit", rf.stateString())

	// 1. election timeout, turn to candidate
	// 2. receive AppendEntries, append log
	// 3. receive RequestVote, do voting
	for {
		select {
		case <-rf.termChangedCh:
			return
		case <-rf.shutdownCh:
			return
		case <-time.After(rf.electionTO()):
			rf.state.changeRole(candidate)
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
