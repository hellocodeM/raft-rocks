package raft

import (
	"time"
)

func (rf *Raft) doCandidate() {
	rf.logInfo("Start leader election")
	defer rf.logInfo("Quit leader election")

	rf.Lock()
	rf.votedFor = int32(rf.me)
	rf.currentTerm++
	rf.persist()
	rf.Unlock()

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
			rf.logInfo("LeaderElection timeout")
			return
		case ok := <-votedCh:
			if ok {
				rf.Lock()
				rf.role = leader
				rf.Unlock()
				rf.logInfo("Leader election succeed")
				return
			}
			rf.logInfo("Leader election fail")
		case s := <-rf.requestVoteChan:
			rf.processRequestVote(s)
		case s := <-rf.appendEntriesCh:
			rf.processAppendEntries(s)
			if s.args.Term >= rf.currentTerm {
				rf.logInfo("Candidate receive leader heartbeat")
				return
			}
		case <-rf.snapshotCh:
			panic("not implemented")
		}
	}
}
