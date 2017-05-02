package raft

import (
	"sync"
	"time"

	"github.com/HelloCodeMing/raft-rocks/common"
	"github.com/golang/glog"
)

// Leader
// 1. Send heartbeat periodically
// 2. If command received from client, append to local log, replicate to all.
// 3. If last log index >= nextIndex for a follower, send AppendEntries RPC with log start at nextIndex
// 4. If exists N > commitIndex, and majority of matchIndex[i] >= N and log[N].term == currentTerm, set commitIndex=N
// So, use replicator to maintain connection between followers, including send heartbeat and replicate log.
// Use committer to update commitIndex, and apply command to state machine(applyCh).
// Connect replicator and committer through a channel
func (rf *Raft) doLeader() {
	rf.logInfo("Become leader")
	defer rf.logInfo("Leader quit.")

	rf.Lock()
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.log.Length()
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitCh = make(chan bool)
	rf.submitCh = make(chan bool, 10)
	rf.Unlock()

	quitCh := make(chan bool)
	done := new(sync.WaitGroup)
	defer done.Wait()
	defer close(quitCh)

	done.Add(len(rf.peers))
	rf.foreachPeer(func(peer int) {
		go rf.replicator(peer, quitCh, done)
	})
	go rf.committer(quitCh, done)

	// TODO it's a temporary work around
	if len(rf.peers) == 1 {
		go func() {
			for range rf.submitCh {
			}
		}()
	}

	for {
		select {
		case <-rf.termChangedCh:
			return
		case <-rf.shutdownCh:
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

// Check leader's commit index, update it if needed
func (rf *Raft) committer(quitCh <-chan bool, done *sync.WaitGroup) {
	defer done.Done()
	glog.Infof("Committer start")
	defer glog.Infof("Committer quit.")

	const interval = 200 * time.Millisecond
	for {
		select {
		case <-quitCh:
			return
		case <-time.After(interval):
		case <-rf.commitCh:
		}
		if rf.leaderCommit() {
			rf.checkApply()
		}
	}
}

// update  commit index, use matchIndex from peers
func (rf *Raft) leaderCommit() (updated bool) {
	rf.Lock()
	defer rf.Unlock()
	lowerIndex := rf.commitIndex + 1
	upperIndex := 0
	// find max matchIndex
	for _, x := range rf.matchIndex {
		if x > upperIndex {
			upperIndex = x
		}
	}
	// if N > commitIndex, a majority of match[i] >= N, and log[N].term == currentTerm
	// set commitIndex = N
	for N := upperIndex; N >= lowerIndex && rf.log.At(N).Term == rf.currentTerm; N-- {
		// count match[i] >= N
		cnt := 1
		for i, x := range rf.matchIndex {
			if i != rf.me && x >= N {
				cnt++
			}
		}
		if cnt >= rf.majority() {
			rf.commitIndex = N
			glog.V(common.VDebug).Infof("Leader update commitIndex: %d", rf.commitIndex)
			updated = true
			break
		}
	}
	return
}
