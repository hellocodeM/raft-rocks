package raft

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/HelloCodeMing/raft-rocks/common"
	"github.com/golang/glog"
	"golang.org/x/net/trace"
)

var (
	reportRaftState bool
)

func init() {
	flag.BoolVar(&reportRaftState, "report_raft_state", false, "report raft state")
}

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	sync.RWMutex
	peers     []*common.ClientEnd
	persister *Persister
	me        int // index into peers[]
	applyCh   chan ApplyMsg

	// persistent state
	votedFor    int32 // to prevent one follower vote for multi candidate, when then restart
	currentTerm int32
	log         *LogManager

	// volatile state
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// volatile state on leaders, reinitialized after election
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // replicated entry index

	// rpc channel
	appendEntriesCh chan *AppendEntriesSession
	requestVoteChan chan *RequestVoteSession
	snapshotCh      chan *installSnapshotSession

	// additional state
	role          raftRole
	termChangedCh chan bool
	submitCh      chan bool // trigger replicator
	commitCh      chan bool // trigger commiter
	shutdownCh    chan bool // shutdown all components

	// snapshot state
	snapshotCB        func(int, int32) // call kvserver to snapshot state to persister
	maxStateSize      int
	lastIncludedIndex int // snapshotted log index
	lastIncludedTerm  int32

	tracer trace.EventLog
}

// Raft roles
type raftRole int

const (
	follower raftRole = iota
	candidate
	leader
)

func (role raftRole) String() string {
	switch role {
	case follower:
		return "follower"
	case candidate:
		return "candidate"
	case leader:
		return "leader"
	default:
		panic("no such role")
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.LittleEndian, rf.votedFor)
	err = binary.Write(buff, binary.LittleEndian, rf.currentTerm)
	err = rf.log.Encode(buff)
	if err != nil {
		log.Fatal(err)
	}
	rf.persister.SaveRaftState(buff.Bytes())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	buff := bytes.NewBuffer(data)
	err1 := binary.Read(buff, binary.LittleEndian, &rf.votedFor)
	err2 := binary.Read(buff, binary.LittleEndian, &rf.currentTerm)
	err3 := rf.log.Decode(buff)
	if err1 != nil || err2 != nil || err3 != nil {
		panic("read state failed")
	}
}

func (rf *Raft) majority() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) String() string {
	rf.RLock()
	defer rf.RUnlock()
	return fmt.Sprintf(
		"raft{id: %d, role: %v, T: %d, voteFor: %d, commitIndex: %d, applied: %d, lastIncluedIndex: %d, lastIncludedTerm: %d, log: %v, nextIndex: %v}",
		rf.me, rf.role, rf.currentTerm, rf.votedFor, rf.commitIndex, rf.lastApplied, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.log, rf.nextIndex)
}

func (rf *Raft) stateString() string {
	return fmt.Sprintf("<%d:%d>", rf.me, rf.currentTerm)
}

func (rf *Raft) logInfo(format string, v ...interface{}) {
	s := fmt.Sprintf(format, v...)
	n := len(rf.peers) + 1
	rf.tracer.Printf(s)
	glog.V(common.VDebug).Infof("<%*d,%*d>: %s", rf.me+1, rf.me, n-rf.me-1, rf.currentTerm, s)
}

// any command to apply
func (rf *Raft) checkApply() {
	rf.Lock()
	defer rf.Unlock()
	old := rf.lastApplied
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		if rf.log.LastIndex() < rf.lastApplied {
			rf.Unlock()
			panic(rf.String())
		}
		msg := ApplyMsg{
			Index:       rf.lastApplied,
			Command:     rf.log.At(rf.lastApplied).Command,
			UseSnapshot: false,
			Snapshot:    []byte{},
		}
		rf.applyCh <- msg
	}
	if rf.lastApplied > old {
		rf.logInfo("Applyed commands until index=%d", rf.lastApplied)
	}
}

// exists a new term
func (rf *Raft) checkNewTerm(candidateID int32, newTerm int32) (beFollower bool) {
	rf.Lock()
	defer rf.Unlock()
	if newTerm > rf.currentTerm {
		rf.logInfo("RuleForAll: find new term<%d,%d>, become follower", candidateID, newTerm)
		rf.beFollower(-1, newTerm)
		rf.termChangedCh <- true
		return true
	}
	return false
}

func electionTO() time.Duration {
	return time.Duration(rand.Int63()%((electionTimeoutMax - electionTimeoutMin).Nanoseconds())) + electionTimeoutMin
}

func (rf *Raft) foreachPeer(f func(peer int)) {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			f(i)
		}
	}
}

// Start raft state machine.
func (rf *Raft) startStateMachine() {
	for {
		select {
		case <-rf.shutdownCh:
			rf.logInfo("stop state machine")
			return
		default:
		}
		rf.makeTracer()
		switch rf.role {
		case follower:
			rf.doFollower()
		case leader:
			rf.doLeader()
		case candidate:
			rf.doCandidate()
		}
	}
}

func (rf *Raft) makeTracer() {
	rf.RLock()
	defer rf.RUnlock()
	if rf.tracer != nil {
		rf.tracer.Finish()
	}
	rf.tracer = trace.NewEventLog("Raft", fmt.Sprintf("peer<%d,%d>", rf.me, rf.currentTerm))
}

// NewRaft
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func NewRaft(peers []*common.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := new(Raft)
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.votedFor = -1
	rf.currentTerm = 0
	rf.log = NewLogManager()
	rf.role = follower
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.requestVoteChan = make(chan *RequestVoteSession)
	rf.appendEntriesCh = make(chan *AppendEntriesSession)
	rf.snapshotCh = make(chan *installSnapshotSession)
	rf.shutdownCh = make(chan bool)
	rf.termChangedCh = make(chan bool, 1)
	rf.makeTracer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.recoverSnapshot()

	rf.logInfo("Created raft instance: %s", rf.String())

	go rf.startStateMachine()
	go rf.reporter()
	// go rf.snapshoter()
	return rf
}

func (raft *Raft) reporter() {
	if !reportRaftState {
		return
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			raft.logInfo("Raft state: %v", raft)
		case <-raft.shutdownCh:
			return
		}
	}
}

func (rf *Raft) Kill() {
	rf.logInfo("Killing raft, wait for goroutines to quit")
	close(rf.shutdownCh)
}

// Submit a command to raft
// The command will be replicated to followers, then leader commmit, applied to the state machine by raftKV,
// and finally response to the client
func (rf *Raft) SubmitCommand(command interface{}) (index int, term int, isLeader bool) {
	rf.Lock()
	defer rf.Unlock()
	if rf.role != leader {
		return -1, -1, false
	}
	isLeader = true

	// append to local log
	logEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log.Append(logEntry)
	index = rf.log.LastIndex()
	term = int(rf.currentTerm)
	rf.matchIndex[rf.me] = index
	rf.persist()

	rf.logInfo("SubmitCommand by leader, {index: %d, command: %v}", index, command)
	rf.submitCh <- true
	if len(rf.peers) == 1 {
		rf.commitCh <- true
	}
	return
}

// return currentTerm and whether this server, currentLeader
// believes it is the leader.
func (rf *Raft) GetState() (currentTerm int, isLeader bool) {
	rf.RLock()
	defer rf.RUnlock()
	currentTerm = int(rf.currentTerm)
	isLeader = rf.role == leader
	return
}

func init() {
	log.SetFlags(log.Lmicroseconds)
}

func (rf *Raft) SetSnapshot(snapshotCB func(int, int32)) {
	rf.snapshotCB = snapshotCB
}

func (rf *Raft) SetMaxStateSize(size int) {
	rf.maxStateSize = size
}
