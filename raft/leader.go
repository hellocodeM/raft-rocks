package raft

import (
	"fmt"
	"time"

	"github.com/golang/glog"
)

// Leader
// 1. Send heartbeat periodically
// 2. If command received from client, append to local log, replicate to all.
// 3. If last log index >= nextIndex for a follower, send AppendEntries RPC with log start at nextIndex
// 4. If exists N > commitIndex, and majority of matchIndex[i] >= N and log[N].term == currentTerm, set commitIndex=N
// So, use replicator to maintain connection between followers, including send heartbeat and replicate log.
func (rf *Raft) doLeader() {
	glog.Infof("%s Become leader", rf)

	rf.state.becomeLeader()
	peerChs := make([]chan struct{}, len(rf.peers))
	defer func() {
		glog.Infof("%s Before leader quit, notify all peers to quit", rf)
		for _, peerCh := range peerChs {
			close(peerCh)
		}
		glog.Infof("%s Leader quit.", rf)
	}()

	for peer := range peerChs {
		peerChs[peer] = make(chan struct{})
		go rf.replicator(peer, peerChs[peer])
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
		case <-rf.submitedCh:
			drainOut(rf.submitedCh)
			// fan-out to each peer
			for _, peerCh := range peerChs {
				peerCh <- struct{}{}
			}
		}
	}
}

func drainOut(ch <-chan int) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// replicator: trigger by peerCh, replicate log until nextIndex to each peer
func (rf *Raft) replicator(peer int, lastIndexCh <-chan struct{}) {
	peerStr := fmt.Sprintf("peer<%d>", peer)
	glog.Infof("%s Start replicator for %s", rf, peerStr)
	defer glog.Infof("%s Quit Replicator of %s quit", rf, peerStr)
	if peer == rf.me {
		rf.localReplicator(lastIndexCh)
		return
	}
	var retreatCnt int32
	rf.replicateLog(peer, &retreatCnt)
	glog.Info(rf, " Send initial heartbeat to ", peerStr)
	for {
		select {
		case _, more := <-lastIndexCh:
			if !more {
				return
			}
		case <-time.After(heartbeatTO):
		}
		rf.replicateLog(peer, &retreatCnt)
	}
}

func (rf *Raft) localReplicator(lastIndexCh <-chan struct{}) {
	for {
		select {
		case _, more := <-lastIndexCh:
			if !more {
				return
			}
		}
		rf.state.replicatedToPeer(rf.me, rf.log.LastIndex())
	}
}
