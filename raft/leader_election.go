package raft

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/HelloCodeMing/raft-rocks/pb"
	"github.com/HelloCodeMing/raft-rocks/utils"
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

type RequestVoteSession struct {
	args  *pb.RequestVoteReq
	reply *pb.RequestVoteRes
	tr    trace.Trace
	done  chan bool
}

func NewRequestVoteSession(me int, args *pb.RequestVoteReq, reply *pb.RequestVoteRes) *RequestVoteSession {
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
	glog.V(utils.VDump).Infof("tracing RequestVote: %s", fmt.Sprintf(format, arg...))
}

func (rf *Raft) RequestVote(ctx context.Context, req *pb.RequestVoteReq) (res *pb.RequestVoteRes, err error) {
	s := NewRequestVoteSession(rf.me, req, res)
	s.trace("args: %+v", *req)
	res.VoteGranted = false
	rf.requestVoteChan <- s
	<-s.done

	s.tr.Finish()
	return nil, nil
}

func (rf *Raft) processRequestVote(session *RequestVoteSession) {
	args := session.args
	reply := session.reply
	rf.checkNewTerm(args.CandidateId, args.Term)

	rf.state.Lock()
	defer rf.state.Unlock()
	voteFor := rf.state.VotedFor
	voteForOk := voteFor == -1 || voteFor == args.CandidateId
	var lastTerm int32
	if rf.lastIncludedIndex < rf.log.LastIndex() {
		lastTerm = rf.log.Last().Term
	} else {
		lastTerm = rf.lastIncludedTerm
	}
	logOk := args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && int(args.LastLogIndex) >= rf.log.LastIndex())

	reply.Term = rf.state.CurrentTerm
	if args.Term >= rf.state.CurrentTerm && voteForOk && logOk {
		rf.state.becomeFollowerUnlocked(args.CandidateId, args.Term)
		reply.VoteGranted = true
		session.trace("GrantVote to candidate<%d,%d>", args.CandidateId, args.Term)
	} else {
		reply.VoteGranted = false
		if args.Term < rf.state.CurrentTerm {
			session.trace("Not grant vote to <%d,%d>, because term %d < %d", args.Term, rf.state.CurrentTerm)
		}
		if !voteForOk {
			session.trace("Not grant vote to <%d,%d>, because votedFor: %d", args.CandidateId, args.Term, voteFor)
		}
		if !logOk {
			session.trace("Not grant vote to <%d,%d>, because logMismatch: %v", args.CandidateId, args.Term, rf.log)
		}
	}
	session.done <- true
}

func (rf *Raft) sendRequestVote(peer int, req *pb.RequestVoteReq) (*pb.RequestVoteRes, error) {
	res, err := rf.peers[peer].RequestVote(context.TODO(), req)
	if err != nil {
		glog.Warningf("Call peer<%d>'s Raft.RequestVote failed,error=%v", peer, err)
		return res, err
	}
	rf.checkNewTerm(int32(peer), res.Term)
	return res, err
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
	args := &pb.RequestVoteReq{
		Term:         term,
		CandidateId:  int32(rf.me),
		LastLogIndex: int32(lastIndex),
		LastLogTerm:  lastTerm,
	}

	grantedCh := make(chan bool, len(rf.peers)-1)
	rf.foreachPeer(func(peer int) {
		go func() {
			res, err := rf.sendRequestVote(peer, args)
			if err == nil && res.VoteGranted {
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
