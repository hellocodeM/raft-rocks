package raft

import (
	"sync"
	"sync/atomic"
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
	glog.Infof("%s Become leader", rf.stateString())
	defer glog.Infof("%s Leader quit.", rf.stateString())

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

// Replicate command to all peers.
func (rf *Raft) replicator(peer int, quitCh <-chan bool, done *sync.WaitGroup) {
	defer done.Done()
	rf.logInfo("Replicator start for peer %d", peer)
	defer rf.logInfo("Replicator for peer %d quit", peer)

	const retreatLimit = 4
	var retreatCnt int32 = 1 // for fast retreat

	// initial heartbeat
	go rf.replicateLog(peer, &retreatCnt)
	for {
		// 1. no submit, periodically check if exists log to replicate
		// 2. exists submit, replicateLog
		select {
		case <-quitCh:
			return
		case <-time.After(time.Duration(heartbeatTO)):
		case <-rf.submitCh:
		}

		go func() {
			// when to send snapshot
			// 1. retreat too many times
			// 2. nextIndex reach lastIncluded
			snapshot := rf.lastIncludedIndex > 0 &&
				(atomic.LoadInt32(&retreatCnt) > retreatLimit || rf.nextIndex[peer] <= rf.lastIncludedIndex)
			if snapshot {
				rf.sendSnapshotTo(peer)
				atomic.StoreInt32(&retreatCnt, 1)
			} else {
				rf.replicateLog(peer, &retreatCnt)
			}
			rf.commitCh <- true
		}()
	}
}
