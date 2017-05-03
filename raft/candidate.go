package raft

import (
	"time"

	"github.com/golang/glog"
)

func (rf *Raft) doCandidate() {
	glog.Infof("%s Start leader election", rf.stateString())
	defer glog.Infof("%s Quit leader election", rf.stateString())

	newTerm := rf.state.becomeCandidate()
	votedCh := make(chan bool)
	go rf.requestingVote(votedCh)

	// 1. voted by majority: succeed then quit
	// 2. find a new leader: become a follower and quit.
	// 3. timeout, enter next election
	timeout := time.After(rf.electionTO())
	for {
		select {
		case <-rf.termChangedCh:
			return
		case <-rf.shutdownCh:
			return
		case <-timeout:
			glog.Infof("%s LeaderElection timeout", rf.stateString())
			return
		case ok := <-votedCh:
			if ok {
				rf.state.becomeLeader()
				glog.Infof("%s Leader election succeed", rf.stateString())
				return
			}
			glog.Infof("%s Leader election fail", rf.stateString())
		case s := <-rf.requestVoteChan:
			rf.processRequestVote(s)
		case s := <-rf.appendEntriesCh:
			rf.processAppendEntries(s)
			if s.args.Term >= newTerm {
				glog.Infof("%s Candidate receive leader heartbeat", rf.stateString())
				return
			}
		case <-rf.snapshotCh:
			panic("not implemented")
		}
	}
}
