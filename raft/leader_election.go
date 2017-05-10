package raft

import (
	"flag"
	"time"

	"github.com/HelloCodeMing/raft-rocks/pb"
	"github.com/golang/glog"
	"golang.org/x/net/context"
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
	ctx   context.Context
	tr    trace.Trace
	done  chan bool
}

func NewRequestVoteSession(me int, ctx context.Context, args *pb.RequestVoteReq, reply *pb.RequestVoteRes) *RequestVoteSession {
	tr, ok := trace.FromContext(ctx)
	if !ok {
		// tr = trace.New("Raft.RequestVote", fmt.Sprintf("peer<%d>", me))
		glog.Fatal("no session in context")
	}
	return &RequestVoteSession{
		args:  args,
		reply: reply,
		tr:    tr,
		done:  make(chan bool, 1),
	}
}

func (session *RequestVoteSession) trace(format string, arg ...interface{}) {
	session.tr.LazyPrintf(format, arg...)
}

func (rf *Raft) RequestVote(ctx context.Context, req *pb.RequestVoteReq) (res *pb.RequestVoteRes, err error) {
	res = &pb.RequestVoteRes{}
	s := NewRequestVoteSession(rf.me, ctx, req, res)
	s.trace("requestVoteChan <- session")
	rf.requestVoteChan <- s
	<-s.done

	// s.tr.Finish()
	return res, nil
}

func (rf *Raft) processRequestVote(session *RequestVoteSession) {
	session.trace("process RequestVote")
	args := session.args
	reply := session.reply
	rf.checkNewTerm(args.CandidateId, args.Term)

	rf.state.Lock()
	defer rf.state.Unlock()
	voteFor := rf.state.VotedFor
	voteForOk := voteFor == -1 || voteFor == args.CandidateId
	lastTerm := rf.log.Last().Term
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
	session.trace("done")
	session.done <- true
}

func (rf *Raft) sendRequestVote(peer int, req *pb.RequestVoteReq) (*pb.RequestVoteRes, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	res, err := rf.peers[peer].RequestVote(ctx, req)
	if err != nil {
		glog.Warningf("%s Call peer<%d>'s Raft.RequestVote failed,error=%v", rf, peer, err)
		return res, err
	}
	rf.checkNewTerm(int32(peer), res.Term)
	return res, err
}

func (rf *Raft) requestingVote(votedCh chan<- bool) {
	if len(rf.peers) == 1 {
		glog.Infoln(rf, " Single node, be leader directly")
		votedCh <- true
		return
	}
	glog.Infoln(rf, " Requesting votes in parallel")

	term := rf.state.getTerm()
	last := rf.log.Last()
	lastIndex := last.Index
	lastTerm := last.Term
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
		glog.Info(rf, " Requesting vote success, voted by majority")
	} else {
		votedCh <- false
		glog.Info(rf, " Requesting vote fail, unvoted by majority")
	}

	glog.Infof("%s RequestingVote end: %d/%d", rf, votedCnt, len(rf.peers))
}
