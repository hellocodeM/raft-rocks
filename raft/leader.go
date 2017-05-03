package raft

import (
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
func (rf *Raft) doLeader() {
	glog.Infof("%s Become leader", rf.stateString())

	rf.state.becomeLeader()
	peerChs := make([]chan struct{}, len(rf.peers))
	defer func() {
		glog.Infof("%s Before leader quit, notify all peers to quit", rf.stateString())
		for _, peerCh := range peerChs {
			close(peerCh)
		}
		glog.Infof("%s Leader quit.", rf.stateString())
	}()

	for peer, peerCh := range peerChs {
		go rf.replicator(peer, peerCh)
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
	glog.V(common.VDebug).Infof("Relicator for peer<%d>", peer)
	defer glog.V(common.VDebug).Infof("Relicator of peer<%d> quit", peer)
	var retreatCnt int32
	for {
		select {
		case _, more := <-lastIndexCh:
			if !more {
				return
			}
		case <-time.After(heartbeatTO):
		}
		// TODO divided into localReplicator and remoteReplicator
		if peer != rf.me {
			rf.replicateLog(peer, &retreatCnt)
		} else {
			rf.state.replicatedToPeer(peer, rf.log.LastIndex())
		}
	}
}
