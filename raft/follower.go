package raft

import (
	"time"

	"github.com/golang/glog"
)

func (rf *Raft) beFollower(candidateID int32, term int32) {
	rf.currentTerm = term
	rf.votedFor = candidateID
	rf.role = follower
	rf.persist()
}

func (rf *Raft) beFollowerLocked(candidateID int32, term int32) {
	rf.Lock()
	defer rf.Unlock()
	rf.beFollower(candidateID, term)
}

func (rf *Raft) doFollower() {
	glog.Infof("%s Become follower", rf.stateString())
	defer glog.Infof("%s Follower quit", rf.stateString())

	for {
		select {
		case <-rf.termChangedCh:
			return
		case <-rf.shutdownCh:
			return
		case <-time.After(rf.electionTO()):
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
