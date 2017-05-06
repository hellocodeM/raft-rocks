package raft

import (
	"time"

	"github.com/HelloCodeMing/raft-rocks/utils"
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
		case <-rf.snapshotCh:
			panic("not implemented")
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
	glog.V(utils.VDebug).Infof("%s Replicator for peer<%d>", rf, peer)
	defer glog.V(utils.VDebug).Infof("Replicator of peer<%d> quit", peer)
	if peer == rf.me {
		rf.localReplicator(lastIndexCh)
		return
	}
	var retreatCnt int32
	for {
		select {
		case _, more := <-lastIndexCh:
			if !more {
				return
			}
			glog.V(utils.VDebug).Infof("%s Replicate log to peer<%d>", rf, peer)
		case <-time.After(heartbeatTO):
			glog.V(utils.VDebug).Infof("%s Send heartbeat to peer<%d>", rf, peer)
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
