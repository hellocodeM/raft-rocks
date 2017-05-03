package raft

import (
	"sync"

	"github.com/HelloCodeMing/raft-rocks/common"
	"github.com/golang/glog"
)

// Hole state of raft, coordinate the update of thme
type raftState struct {
	sync.Mutex
	raft     *Raft
	me       int
	numPeers int

	matchIndex []int
	nextIndex  []int

	// volatile state
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine
}

func (s *raftState) getApplited() int {
	s.Lock()
	defer s.Unlock()
	return s.lastApplied
}

func (s *raftState) getCommied() int {
	s.Lock()
	defer s.Unlock()
	return s.commitIndex
}

func (s *raftState) checkFollowerCommit(leaderCommit int) bool {
	s.Lock()
	defer s.Unlock()
	if leaderCommit > s.commitIndex {
		s.commitIndex = leaderCommit
		s.checkApply()
		return true
	}
	return false
}

// any command to apply
func (s *raftState) checkApply() {
	old := s.lastApplied
	rf := s.raft
	for s.commitIndex > s.lastApplied {
		s.lastApplied++
		if rf.log.LastIndex() < s.lastApplied {
			panic(rf.String())
		}
		entry := rf.log.At(s.lastApplied)
		msg := ApplyMsg{
			Index:       s.lastApplied,
			Command:     entry.Command,
			Term:        entry.Term,
			UseSnapshot: false,
			Snapshot:    []byte{},
		}
		rf.applyCh <- msg
	}
	if s.lastApplied > old {
		glog.V(common.VDebug).Infof("Applyed commands until index=%d", s.lastApplied)
	}
}

func (s *raftState) becomeLeader() {
	s.Lock()
	defer s.Unlock()
	s.matchIndex = make([]int, s.numPeers)
	s.nextIndex = make([]int, s.numPeers)
}

func (s *raftState) replicatedToPeer(peer int, index int) {
	s.Lock()
	defer s.Unlock()
	s.matchIndex[peer] = maxInt(index, s.matchIndex[peer])
	s.nextIndex[peer] = s.matchIndex[peer] + 1
	if s.checkLeaderCommit() {
		s.checkApply()
	}
}

// update  commit index, use matchIndex from peers
// NOTE: It's not thread-safe, should be synchronized by external lock
func (s *raftState) checkLeaderCommit() (updated bool) {
	rf := s.raft
	lowerIndex := s.commitIndex + 1
	upperIndex := 0
	// find max matchIndex
	for _, x := range s.matchIndex {
		if x > upperIndex {
			upperIndex = x
		}
	}
	// if N > commitIndex, a majority of match[i] >= N, and log[N].term == currentTerm
	// set commitIndex = N
	for N := upperIndex; N >= lowerIndex && rf.log.At(N).Term == rf.currentTerm; N-- {
		// count match[i] >= N
		cnt := 1
		for i, x := range s.matchIndex {
			if i != rf.me && x >= N {
				cnt++
			}
		}
		if cnt >= rf.majority() {
			s.commitIndex = N
			glog.V(common.VDebug).Infof("Leader update commitIndex: %d", s.commitIndex)
			updated = true
			break
		}
	}
	return
}

func (s *raftState) retreatForPeer(peer int) int {
	s.Lock()
	defer s.Unlock()
	idx := &s.nextIndex[peer]
	rf := s.raft
	*idx = minInt(*idx, rf.log.LastIndex())
	if *idx > rf.lastIncludedIndex {
		oldTerm := rf.log.At(*idx).Term
		for *idx > rf.lastIncludedIndex+1 && rf.log.At(*idx).Term == oldTerm {
			*idx--
		}
	}
	*idx = maxInt(*idx, rf.lastIncludedIndex+1)
	return *idx
}

func (s *raftState) toReplicate(peer int) int {
	s.Lock()
	defer s.Unlock()
	return s.nextIndex[peer]
}

func makeRaftState(raft *Raft, numPeers int, me int) *raftState {
	return &raftState{
		raft:     raft,
		numPeers: numPeers,
		me:       me,
	}
}
