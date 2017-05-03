package raft

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/HelloCodeMing/raft-rocks/common"
	"github.com/golang/glog"
)

// Hold state of raft, coordinate the updating and reading
type raftState struct {
	sync.RWMutex
	raft     *Raft
	me       int
	numPeers int
	role     raftRole

	currentTerm int32
	votedFor    int32 // to prevent one follower vote for multi candidate, when then restart

	matchIndex []int
	nextIndex  []int

	// volatile state
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	readLease time.Time
}

// maybe we could cache this field in Raft, to avoid lock
func (s *raftState) getTerm() int32 {
	s.RLock()
	defer s.RUnlock()
	return s.currentTerm
}

func (s *raftState) getCommied() int {
	s.RLock()
	defer s.RUnlock()
	return s.commitIndex
}

func (s *raftState) persist() {
	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.LittleEndian, s.votedFor)
	err = binary.Write(buff, binary.LittleEndian, s.currentTerm)
	err = s.raft.log.Encode(buff)
	if err != nil {
		glog.Fatal(err)
	}
	s.raft.persister.SaveRaftState(buff.Bytes())
}

// restore previously persisted state.
func (s *raftState) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	buff := bytes.NewBuffer(data)
	err1 := binary.Read(buff, binary.LittleEndian, &s.votedFor)
	err2 := binary.Read(buff, binary.LittleEndian, &s.currentTerm)
	err3 := s.raft.log.Decode(buff)
	if err1 != nil || err2 != nil || err3 != nil {
		glog.Fatal("read state failed")
	}
}

func (s *raftState) checkNewTerm(newTerm int32) bool {
	if s.getTerm() < newTerm {
		s.becomeFollower(-1, newTerm)
		return true
	}
	return false
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
		glog.V(common.VDebug).Infof("%s Applyed commands until index=%d", s.String(), s.lastApplied)
	}
}

func (s *raftState) changeRole(role raftRole) {
	s.Lock()
	defer s.Unlock()
	s.role = role
}

func (s *raftState) becomeFollower(candidateID int32, term int32) {
	s.Lock()
	defer s.Unlock()
	s.currentTerm = term
	s.votedFor = candidateID
	s.role = follower
	s.persist()
}

func (s *raftState) becomeCandidate() int32 {
	s.Lock()
	defer s.Unlock()
	s.role = candidate
	s.currentTerm++
	s.votedFor = int32(s.me)
	s.persist()
	return s.currentTerm
}

func (s *raftState) becomeLeader() {
	s.Lock()
	defer s.Unlock()
	s.role = leader
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
	for N := upperIndex; N >= lowerIndex && rf.log.At(N).Term == s.currentTerm; N-- {
		// count match[i] >= N
		cnt := 1
		for i, x := range s.matchIndex {
			if i != rf.me && x >= N {
				cnt++
			}
		}
		if cnt >= rf.majority() {
			s.commitIndex = N
			glog.V(common.VDebug).Infof("%s Leader update commitIndex: %d", s.String(), s.commitIndex)
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

// If lease is granted in this term, and later than the old one, extend the lease
func (s *raftState) updateReadLease(term int32, lease time.Time) {
	s.Lock()
	defer s.Unlock()
	if term == s.currentTerm && lease.After(s.readLease) {
		s.readLease = lease
		glog.Infof("%s Update read lease to %v", s.String(), lease)
	}
}

func (s *raftState) String() string {
	return fmt.Sprintf("Raft<%d:%d>", s.me, s.currentTerm)
}

func makeRaftState(raft *Raft, numPeers int, me int) *raftState {
	return &raftState{
		raft:        raft,
		numPeers:    numPeers,
		me:          me,
		currentTerm: 0,
		votedFor:    -1,
		role:        follower,
		readLease:   time.Now(),
	}
}
