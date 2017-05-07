package raft

import (
	"time"

	"github.com/HelloCodeMing/raft-rocks/pb"
	"github.com/golang/glog"
)

func (rf *Raft) doFollower() {
	glog.Info(rf, " Become follower")
	defer glog.Info(rf, " Follower quit")

	// 1. election timeout, turn to candidate
	// 2. receive AppendEntries, append log
	// 3. receive RequestVote, do voting
	timeout := rf.electionTO()
	for {
		select {
		case <-rf.termChangedCh:
			return
		case <-rf.shutdownCh:
			return
		case <-time.After(timeout):
			rf.state.changeRole(pb.RaftRole_Candidate)
			glog.Infof("%s Follower lose heartbeat for %s, be candidate", rf, timeout)
			return
		case s := <-rf.appendEntriesCh:
			rf.processAppendEntries(s)
		case s := <-rf.requestVoteChan:
			rf.processRequestVote(s)
		}
	}
}
