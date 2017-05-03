package raft

import (
	"flag"
	"fmt"
	"time"

	"github.com/HelloCodeMing/raft-rocks/common"
	"github.com/golang/glog"
	"golang.org/x/net/trace"
)

var (
	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
)

func init() {
	flag.DurationVar(&electionTimeoutMin, "election_min", 1*time.Second, "minimum duration of election timeout")
	flag.DurationVar(&electionTimeoutMax, "election_max", 2*time.Second, "maximum duration of election timeout")
}

type RequestVoteArgs struct {
	Term         int32
	CandidateID  int32
	LastLogIndex int
	LastLogTerm  int32
}

type RequestVoteReply struct {
	Term        int32 // currentTerm, for candidate to update itself
	VoteGranted bool  // whether receive vote
}

type RequestVoteSession struct {
	args  *RequestVoteArgs
	reply *RequestVoteReply
	tr    trace.Trace
	done  chan bool
}

func NewRequestVoteSession(me int, args *RequestVoteArgs, reply *RequestVoteReply) *RequestVoteSession {
	tr := trace.New("Raft.RequestVote", fmt.Sprintf("peer<%d>", me))
	return &RequestVoteSession{
		args:  args,
		reply: reply,
		tr:    tr,
		done:  make(chan bool, 1),
	}
}

func (session *RequestVoteSession) trace(format string, arg ...interface{}) {
	session.tr.LazyPrintf(format, arg...)
	glog.V(common.VDump).Infof("tracing RequestVote: %s", fmt.Sprintf(format, arg...))
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	s := NewRequestVoteSession(rf.me, args, reply)
	s.trace("args: %+v", *args)
	reply.VoteGranted = false
	rf.requestVoteChan <- s
	<-s.done
	s.tr.Finish()
	return nil
}

func (rf *Raft) processRequestVote(session *RequestVoteSession) {
	args := session.args
	reply := session.reply
	rf.checkNewTerm(args.CandidateID, args.Term)

	rf.state.Lock()
	defer rf.state.Unlock()
	voteFor := rf.state.VotedFor
	voteForOk := voteFor == -1 || voteFor == args.CandidateID
	var lastTerm int32
	if rf.lastIncludedIndex < rf.log.LastIndex() {
		lastTerm = rf.log.Last().Term
	} else {
		lastTerm = rf.lastIncludedTerm
	}
	logOk := args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= rf.log.LastIndex())

	reply.Term = rf.state.CurrentTerm
	if args.Term >= rf.state.CurrentTerm && voteForOk && logOk {
		rf.state.becomeFollowerUnlocked(args.CandidateID, args.Term)
		reply.VoteGranted = true
		session.trace("GrantVote to candidate<%d,%d>", args.CandidateID, args.Term)
	} else {
		reply.VoteGranted = false
		if args.Term < rf.state.CurrentTerm {
			session.trace("Not grant vote to <%d,%d>, because term %d < %d", args.Term, rf.state.CurrentTerm)
		}
		if !voteForOk {
			session.trace("Not grant vote to <%d,%d>, because votedFor: %d", args.CandidateID, args.Term, voteFor)
		}
		if !logOk {
			session.trace("Not grant vote to <%d,%d>, because logMismatch: %v", args.CandidateID, args.Term, rf.log)
		}
	}
	session.done <- true
}

func (rf *Raft) sendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	err := rf.peers[peer].Call("Raft.RequestVote", args, reply)
	if err != nil {
		glog.Warningf("Call peer<%d>'s Raft.RequestVote failed,error=%v", peer, err)
		return false
	}
	rf.checkNewTerm(int32(peer), reply.Term)
	return err == nil
}

func (rf *Raft) requestingVote(votedCh chan<- bool) {
	if len(rf.peers) == 1 {
		glog.Infoln(rf.stateString(), " Single node, be leader directly")
		votedCh <- true
		return
	}
	glog.Infoln(rf.stateString(), " Requesting votes in parallel")

	term := rf.state.getTerm()
	lastIndex := rf.log.LastIndex()
	var lastTerm int32
	if rf.lastIncludedIndex < lastIndex {
		lastTerm = rf.log.Last().Term
	} else {
		lastTerm = rf.lastIncludedTerm
	}
	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  int32(rf.me),
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	grantedCh := make(chan bool, len(rf.peers)-1)
	rf.foreachPeer(func(peer int) {
		go func() {
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(peer, args, reply) && reply.VoteGranted {
				grantedCh <- true
			} else {
				grantedCh <- false
			}
		}()
	})
	votedCnt := 1
	unvoteCnt := 0

	for votedCnt < rf.majority() && unvoteCnt < rf.majority() {
		ok := <-grantedCh
		if ok {
			votedCnt++
		} else {
			unvoteCnt++
		}
	}
	if votedCnt >= rf.majority() {
		votedCh <- true
		rf.logInfo("Requesting vote success, voted by majority")
	} else {
		votedCh <- false
		rf.logInfo("Requesting vote fail, unvoted by majority")
	}

	rf.logInfo("RequestingVote end: %d/%d", votedCnt, len(rf.peers))
}
