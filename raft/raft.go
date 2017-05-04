package raft

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/HelloCodeMing/raft-rocks/common"
	. "github.com/HelloCodeMing/raft-rocks/common"
	"github.com/HelloCodeMing/raft-rocks/store"
	"github.com/golang/glog"
	"golang.org/x/net/trace"
)

var (
	reportRaftState bool
)

func init() {
	flag.BoolVar(&reportRaftState, "report_raft_state", false, "report raft state")
	log.SetFlags(log.Lmicroseconds)
}

type ApplyMsg struct {
	Index       int
	Term        int32
	Command     *common.KVCommand
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	peers []*common.ClientEnd
	me    int // index into peers[]
	rand  *rand.Rand

	log   *store.LogStorage
	state *raftState

	// raft send apply message to RaftKV
	applyCh chan ApplyMsg
	// rpc channel
	appendEntriesCh chan *AppendEntriesSession
	requestVoteChan chan *RequestVoteSession
	snapshotCh      chan *installSnapshotSession

	// once submit a command, send lastLogIndex into this chan,
	// the replicator will try best replicate all logEntries until lastLogIndex
	submitedCh    chan int
	termChangedCh chan bool
	shutdownCh    chan bool // shutdown all components

	// snapshot state
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

func (rf *Raft) majority() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) String() string {
	return "raft"
}

func (rf *Raft) stateString() string {
	return rf.state.String()
}

func (rf *Raft) logInfo(format string, v ...interface{}) {
	s := fmt.Sprintf(format, v...)
	rf.tracer.Printf(s)
	glog.V(common.VDebug).Infof("%s %s", rf.stateString(), s)
}

// exists a new term
func (rf *Raft) checkNewTerm(candidateID int32, newTerm int32) (beFollower bool) {
	if rf.state.checkNewTerm(newTerm) {
		glog.Infof("%s RuleForAll: find new term<%d,%d>, become follower", rf.stateString(), candidateID, newTerm)
		rf.termChangedCh <- true
		return true
	}
	return false
}

func (rf *Raft) electionTO() time.Duration {
	return time.Duration(rf.rand.Int63()%((electionTimeoutMax - electionTimeoutMin).Nanoseconds())) + electionTimeoutMin
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
		switch rf.state.Role {
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
	if rf.tracer != nil {
		rf.tracer.Finish()
	}
	rf.tracer = trace.NewEventLog("Raft", fmt.Sprintf("peer<%d,%d>", rf.me, rf.state.getTerm()))
}

func (rf *Raft) Kill() {
	rf.logInfo("Killing raft, wait for goroutines to quit")
	close(rf.shutdownCh)
}

func (rf *Raft) UpdateReadLease(term int32, lease time.Time) {
	rf.state.updateReadLease(term, lease)
}

// Submit a command to raft
// The command will be replicated to followers, then leader commmit, applied to the state machine by raftKV,
// and finally response to the client
func (rf *Raft) SubmitCommand(command *common.KVCommand) (index int, term int32, isLeader bool) {
	term = rf.state.getTerm()
	isLeader = rf.state.getRole() == leader
	if !isLeader {
		return -1, -1, false
	}
	isLeader = true

	if command.CmdType == common.CmdGet {
		if time.Now().Before(rf.state.readLease) {
			glog.V(common.VDebug).Infof("Get with lease read %s", command.String())
			applyMsg := ApplyMsg{
				Command: command,
			}
			rf.applyCh <- applyMsg
			return
		}
	}
	command.Timestamp = time.Now().UnixNano()
	// append to local log
	logEntry := LogEntry{
		Term:    term,
		Command: command,
	}
	rf.log.Append(logEntry)
	index = rf.log.LastIndex()

	go func() {
		rf.submitedCh <- index
	}()
	rf.logInfo("SubmitCommand by leader, {index: %d, command: %v}", index, command)
	return
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
func NewRaft(peers []*common.ClientEnd, me int, persister *Persister, log *store.LogStorage, applyCh chan ApplyMsg) *Raft {
	rf := new(Raft)
	rf.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.peers = peers
	rf.me = me
	rf.applyCh = applyCh

	rf.log = log
	rf.requestVoteChan = make(chan *RequestVoteSession)
	rf.appendEntriesCh = make(chan *AppendEntriesSession)
	rf.snapshotCh = make(chan *installSnapshotSession)
	rf.shutdownCh = make(chan bool)
	rf.termChangedCh = make(chan bool, 1)
	rf.submitedCh = make(chan int, 10)
	rf.state = makeRaftState(rf, persister, len(peers), me)
	rf.makeTracer()

	// initialize from state persisted before a crash
	rf.state.readPersist(persister.ReadRaftState())
	// rf.recoverSnapshot()

	glog.Infof("%s Created raft instance: %s", rf.stateString(), rf.String())

	go rf.startStateMachine()
	// go rf.snapshoter()
	http.HandleFunc("/raft", func(res http.ResponseWriter, req *http.Request) {
		rf.state.dump(res)
		req.Body.Close()
	})
	return rf
}
