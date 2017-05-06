package raft

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/HelloCodeMing/raft-rocks/pb"
	"github.com/HelloCodeMing/raft-rocks/store"
	"github.com/HelloCodeMing/raft-rocks/utils"
	"github.com/golang/glog"
)

type ApplyMsg struct {
	Command     *pb.KVCommand
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	peers []*utils.ClientEnd
	me    int // index into peers[]

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
}

func (rf *Raft) majority() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) String() string {
	return rf.state.String()
}

func (rf *Raft) logInfo(format string, v ...interface{}) {
	s := fmt.Sprintf(format, v...)
	glog.V(utils.VDebug).Infof("%s %s", rf, s)
}

// exists a new term
func (rf *Raft) checkNewTerm(candidateID int32, newTerm int32) (beFollower bool) {
	if rf.state.checkNewTerm(newTerm) {
		glog.Infof("%s RuleForAll: find new term<%d,%d>, become follower", rf, candidateID, newTerm)
		rf.termChangedCh <- true
		return true
	}
	return false
}

func (rf *Raft) electionTO() time.Duration {
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
		switch rf.state.Role {
		case pb.RaftRole_Follower:
			rf.doFollower()
		case pb.RaftRole_Candidate:
			rf.doLeader()
		case pb.RaftRole_Leader:
			rf.doCandidate()
		}
	}
}

func (rf *Raft) Kill() {
	rf.logInfo("Killing raft, wait for goroutines to quit")
	close(rf.shutdownCh)
}

func (rf *Raft) UpdateReadLease(term int32, lease time.Time) {
	rf.state.updateReadLease(term, lease)
}

// SubmitCommand submit a command to raft
// The command will be replicated to followers, then leader commmit, applied to the state machine by raftKV,
// and finally response to the client
func (rf *Raft) SubmitCommand(ctx context.Context, command *pb.KVCommand) (isLeader bool) {
	term := rf.state.getTerm()
	isLeader = rf.state.getRole() == pb.RaftRole_Leader
	if !isLeader {
		return false
	}
	isLeader = true
	command.Timestamp = time.Now().UnixNano()
	command.Term = term

	if command.GetCmdType() == pb.CommandType_Get {
		if time.Now().Before(rf.state.readLease) {
			glog.V(utils.VDebug).Infof("Get with lease read %s", command.String())
			applyMsg := ApplyMsg{
				Command: command,
			}
			rf.applyCh <- applyMsg
			return
		}
	}
	// append to local log
	index := rf.log.Append(command)
	command.Index = int32(index)

	go func() {
		rf.submitedCh <- index
	}()
	rf.logInfo("SubmitCommand by leader, {index: %d, command: %v}", index, command)
	return
}

func (rf *Raft) registerDebugHandler() {
	// a http interface to dump raft state, for debug purpose
	http.HandleFunc("/raft", func(res http.ResponseWriter, req *http.Request) {
		rf.state.dump(res)
		req.Body.Close()
	})
}

// NewRaft create a raft instance
// peers used to communicate with other peers in this raft group, need to be construct in advance
// persister used to store metadata of raft, and log used for WAL
// applyCh, apply a command to state machine through this channel
func NewRaft(peers []*utils.ClientEnd, me int, persister store.Persister, log *store.LogStorage, applyCh chan ApplyMsg) *Raft {
	rf := new(Raft)
	rf.peers = peers
	rf.me = me
	rf.applyCh = applyCh

	rf.log = log
	rf.requestVoteChan = make(chan *RequestVoteSession, 10)
	rf.appendEntriesCh = make(chan *AppendEntriesSession, 100)
	rf.shutdownCh = make(chan bool)
	rf.termChangedCh = make(chan bool, 10)
	rf.submitedCh = make(chan int, 10)
	rf.state = makeRaftState(rf, persister, len(peers), me)

	glog.Infof("%s Created raft instance: %s", rf, rf.String())
	go rf.startStateMachine()
	rf.registerDebugHandler()

	return rf
}
