package raft

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"

	"encoding/json"

	"github.com/HelloCodeMing/raft-rocks/common"
	"github.com/golang/glog"
)

// Hold state of raft, coordinate the updating and reading
type raftState struct {
	sync.RWMutex
	raft      *Raft
	persister *Persister

	// once initialized, they will not be changed
	Me       int
	NumPeers int

	Role        raftRole
	CurrentTerm int32
	VotedFor    int32

	// maintained by leader, record follower's repliate and commit state
	MatchIndex []int
	NextIndex  []int

	// volatile state
	CommitIndex int // index of highest log entry known to be committed
	LastApplied int // index of highest log entry applied to state machine

	readLease time.Time
}

func (s *raftState) getRole() raftRole {
	s.RLock()
	defer s.RUnlock()
	return s.Role
}

// maybe we could cache this field in Raft, to avoid lock
func (s *raftState) getTerm() int32 {
	s.RLock()
	defer s.RUnlock()
	return s.CurrentTerm
}

func (s *raftState) getCommied() int {
	s.RLock()
	defer s.RUnlock()
	return s.CommitIndex
}

func (s *raftState) persist() {
	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.LittleEndian, s.VotedFor)
	err = binary.Write(buff, binary.LittleEndian, s.CurrentTerm)
	if err != nil {
		glog.Fatal(err)
	}
	s.persister.SaveRaftState(buff.Bytes())
}

// restore previously persisted state.
func (s *raftState) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	buff := bytes.NewBuffer(data)
	err1 := binary.Read(buff, binary.LittleEndian, &s.VotedFor)
	err2 := binary.Read(buff, binary.LittleEndian, &s.CurrentTerm)
	if err1 != nil || err2 != nil {
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
	if leaderCommit > s.CommitIndex {
		s.CommitIndex = leaderCommit
		s.checkApply()
		return true
	}
	return false
}

// any command to apply
func (s *raftState) checkApply() {
	old := s.LastApplied
	rf := s.raft
	for s.CommitIndex > s.LastApplied {
		s.LastApplied++
		if rf.log.LastIndex() < s.LastApplied {
			panic(rf.String())
		}
		entry := rf.log.At(s.LastApplied)
		msg := ApplyMsg{
			Index:       s.LastApplied,
			Command:     entry.Command,
			Term:        entry.Term,
			UseSnapshot: false,
			Snapshot:    []byte{},
		}
		rf.applyCh <- msg
	}
	if s.LastApplied > old {
		glog.V(common.VDebug).Infof("%s Applyed commands until index=%d", s.String(), s.LastApplied)
	}
}

func (s *raftState) changeRole(role raftRole) {
	s.Lock()
	defer s.Unlock()
	s.Role = role
}

func (s *raftState) becomeFollowerUnlocked(candidateID int32, term int32) {
	s.CurrentTerm = term
	s.VotedFor = candidateID
	s.Role = follower
	s.persist()
}

func (s *raftState) becomeFollower(candidateID int32, term int32) {
	s.Lock()
	defer s.Unlock()
	s.CurrentTerm = term
	s.VotedFor = candidateID
	s.Role = follower
	s.persist()
}

func (s *raftState) becomeCandidate() int32 {
	s.Lock()
	defer s.Unlock()
	s.Role = candidate
	s.CurrentTerm++
	s.VotedFor = int32(s.Me)
	s.persist()
	return s.CurrentTerm
}

func (s *raftState) becomeLeader() {
	s.Lock()
	defer s.Unlock()
	s.Role = leader
	s.MatchIndex = make([]int, s.NumPeers)
	s.NextIndex = make([]int, s.NumPeers)
}

func (s *raftState) replicatedToPeer(peer int, index int) {
	s.Lock()
	defer s.Unlock()
	s.MatchIndex[peer] = maxInt(index, s.MatchIndex[peer])
	s.NextIndex[peer] = s.MatchIndex[peer] + 1
	if s.checkLeaderCommit() {
		s.checkApply()
	}
}

// update  commit index, use matchIndex from peers
// NOTE: It's not thread-safe, should be synchronized by external lock
func (s *raftState) checkLeaderCommit() (updated bool) {
	rf := s.raft
	lowerIndex := s.CommitIndex + 1
	upperIndex := 0
	// find max matchIndex
	for _, x := range s.MatchIndex {
		if x > upperIndex {
			upperIndex = x
		}
	}
	// if N > commitIndex, a majority of match[i] >= N, and log[N].term == currentTerm
	// set commitIndex = N
	for N := upperIndex; N >= lowerIndex && rf.log.At(N).Term == s.CurrentTerm; N-- {
		// count match[i] >= N
		cnt := 1
		for i, x := range s.MatchIndex {
			if i != rf.me && x >= N {
				cnt++
			}
		}
		if cnt >= rf.majority() {
			s.CommitIndex = N
			glog.V(common.VDebug).Infof("%s Leader update commitIndex: %d", s.String(), s.CommitIndex)
			updated = true
			break
		}
	}
	return
}

func (s *raftState) retreatForPeer(peer int) int {
	s.Lock()
	defer s.Unlock()
	idx := &s.NextIndex[peer]
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
	return s.NextIndex[peer]
}

// If lease is granted in this term, and later than the old one, extend the lease
func (s *raftState) updateReadLease(term int32, lease time.Time) {
	if term == s.CurrentTerm && lease.After(s.readLease) {
		s.readLease = lease
		glog.Infof("%s Update read lease to %v", s.String(), lease)
	}
}

func (s *raftState) String() string {
	return fmt.Sprintf("Raft<%d:%d>", s.Me, s.CurrentTerm)
}

func (s *raftState) dump(writer io.Writer) {
	buf, err := json.MarshalIndent(s, "", "\t")
	if err != nil {
		panic(err)
	}
	writer.Write(buf)
}

func makeRaftState(raft *Raft, persister *Persister, numPeers int, me int) *raftState {
	return &raftState{
		raft:        raft,
		NumPeers:    numPeers,
		Me:          me,
		CurrentTerm: 0,
		VotedFor:    -1,
		Role:        follower,
		readLease:   time.Now(),
		persister:   persister,
	}
}
