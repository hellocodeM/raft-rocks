package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/HelloCodeMing/raft-rocks/pb"
	"github.com/HelloCodeMing/raft-rocks/store"
	"github.com/HelloCodeMing/raft-rocks/utils"
	"github.com/golang/glog"
)

// Hold state of raft, coordinate the updating and reading
type raftState struct {
	sync.RWMutex
	log       *store.LogStorage
	persister store.Persister
	applyCh   chan<- *ApplyMsg

	// once initialized, they will not be changed
	Me       int
	NumPeers int

	Role        pb.RaftRole
	CurrentTerm int32
	VotedFor    int32

	// maintained by leader, record follower's repliate and commit state
	MatchIndex []int
	NextIndex  []int

	// in raft paper, these two field is volatile
	// when restart, we need replay all WAL to recover state
	// otherwise, we have to do checkpoint, and record where has beed commited and applied to state machine
	// but when it comes to RocksDB like storage engine, it has its own WAL, so the storage itself is durable
	// but disappointing, even if reading RocksDB's WAL to get LastApplied is possible, it's too tricky
	// An elegant way to avoid 'double WAL' may be stop use WAL in RocksDB, but only in Raft,
	// and in raft we could perioidly do checkpoint/snapshot, persist LastApplied, as a result, we just need to
	// restore state machine from checkpoint, and replay WAL.
	//
	// All above is just my imagination, the real implementation is too naive. When LastApplied is updated, store it in persister.
	CommitIndex int // index of highest log entry known to be committed
	LastApplied int // index of highest log entry applied to state machine

	readLease time.Time
}

func (s *raftState) getRole() pb.RaftRole {
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

func (s *raftState) getCommited() int {
	s.RLock()
	defer s.RUnlock()
	return s.CommitIndex
}

const (
	currentTerm = "currentTerm"
	votedFor    = "votedFor"
	lastApplied = "lastApplied"
)

func (s *raftState) persist() {
	s.persister.StoreInt32(currentTerm, s.CurrentTerm)
	s.persister.StoreInt32(votedFor, s.VotedFor)
}

func (s *raftState) restore() {
	var ok bool
	s.CurrentTerm, ok = s.persister.LoadInt32(currentTerm)
	s.VotedFor, ok = s.persister.LoadInt32(votedFor)
	if !ok {
		s.VotedFor = -1
	}
	apply, ok := s.persister.LoadInt32(lastApplied)
	if ok {
		s.LastApplied = int(apply)
	}
}

func (s *raftState) commitUntil(index int) {
	s.CommitIndex = index
}

func (s *raftState) applyOne() {
	s.LastApplied++
	s.persister.StoreInt32(lastApplied, int32(s.LastApplied))
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
	// Have uncommited log, but leader has commited them
	if s.CommitIndex < leaderCommit && s.CommitIndex < s.log.LastIndex() {
		s.commitUntil(minInt(leaderCommit, int(s.log.LastIndex())))
		s.checkApply()
		return true
	}
	return false
}

// any command to apply
func (s *raftState) checkApply() {
	old := s.LastApplied
	log := s.log
	for s.CommitIndex > s.LastApplied {
		s.applyOne()
		if log.LastIndex() < s.LastApplied {
			panic(s)
		}
		entry := log.At(s.LastApplied)
		if entry == nil {
			break
		}
		msg := &ApplyMsg{Command: entry}
		s.applyCh <- msg
	}
	if s.LastApplied > old {
		glog.V(utils.VDebug).Infof("%s Applyed commands until index=%d", s.String(), s.LastApplied)
	}
}

func (s *raftState) changeRole(role pb.RaftRole) {
	s.Lock()
	defer s.Unlock()
	s.Role = role

}

func (s *raftState) becomeFollowerUnlocked(candidateID int32, term int32) {
	s.CurrentTerm = term
	s.VotedFor = candidateID
	s.Role = pb.RaftRole_Follower
	s.persist()
}

func (s *raftState) becomeFollower(candidateID int32, term int32) {
	s.Lock()
	defer s.Unlock()
	s.CurrentTerm = term
	s.VotedFor = candidateID
	s.Role = pb.RaftRole_Follower
	s.MatchIndex = nil
	s.NextIndex = nil
	s.persist()
}

func (s *raftState) becomeCandidate() int32 {
	s.Lock()
	defer s.Unlock()
	s.Role = pb.RaftRole_Candidate
	s.CurrentTerm++
	s.VotedFor = int32(s.Me)
	s.MatchIndex = nil
	s.NextIndex = nil
	s.persist()
	return s.CurrentTerm
}

func (s *raftState) becomeLeader() {
	s.Lock()
	defer s.Unlock()
	s.Role = pb.RaftRole_Leader
	s.MatchIndex = make([]int, s.NumPeers)
	s.NextIndex = make([]int, s.NumPeers)
	lastIndex := s.log.LastIndex()
	for i := range s.NextIndex {
		s.NextIndex[i] = lastIndex + 1
	}
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
	for N := upperIndex; N >= lowerIndex && s.log.At(N).Term == s.CurrentTerm; N-- {
		// count match[i] >= N
		cnt := 1
		for i, x := range s.MatchIndex {
			if i != s.Me && x >= N {
				cnt++
			}
		}
		if cnt >= s.majority() {
			s.commitUntil(N)
			glog.V(utils.VDebug).Infof("%s Leader update commitIndex: %d", s.String(), s.CommitIndex)
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
	log := s.log
	*idx = minInt(*idx, log.LastIndex())
	oldTerm := log.At(*idx).Term
	for *idx > 0 && log.At(*idx).Term == oldTerm {
		*idx--
	}
	*idx = maxInt(*idx, 1)
	return *idx
}

func (s *raftState) toReplicate(peer int) int {
	return s.NextIndex[peer]
}

// If lease is granted in this term, and later than the old one, extend the lease
func (s *raftState) updateReadLease(term int32, lease time.Time) {
	if s.readLease.Before(time.Now()) && term == s.CurrentTerm && lease.After(s.readLease) {
		s.readLease = lease
		glog.Infof("%s Update read lease to %v", s.String(), lease)
	}
}

func (s *raftState) String() string {
	return fmt.Sprintf("Raft<%d:%d>", s.Me, s.CurrentTerm)
}

func (s *raftState) dump(writer io.Writer) {
	state := map[string]interface{}{
		"meta":         s,
		"LogLastIndex": s.log.LastIndex(),
	}
	buf, err := json.MarshalIndent(state, "", "\t")
	if err != nil {
		panic(err)
	}
	writer.Write(buf)
}

func (s *raftState) majority() int {
	return s.NumPeers/2 + 1
}

func makeRaftState(log *store.LogStorage, persister store.Persister, applyCh chan<- *ApplyMsg, numPeers int, me int) *raftState {
	s := &raftState{
		log:         log,
		applyCh:     applyCh,
		NumPeers:    numPeers,
		Me:          me,
		CurrentTerm: 0,
		VotedFor:    -1,
		Role:        pb.RaftRole_Follower,
		readLease:   time.Now(),
		persister:   persister,
	}
	s.restore()
	glog.Infof("Restore raft state: CurrentTerm: %d, CommitIndex: %d, LastApplied: %d", s.CurrentTerm, s.CommitIndex, s.LastApplied)
	return s
}
