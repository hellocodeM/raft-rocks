package raft

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/HelloCodeMing/raft-rocks/pb"
	"github.com/HelloCodeMing/raft-rocks/utils"
	"github.com/golang/glog"
	"golang.org/x/net/context"
	"golang.org/x/net/trace"
)

const DumpRPCAppendEntries = false

var (
	heartbeatTO time.Duration
)

func init() {
	flag.DurationVar(&heartbeatTO, "leader_heartbeat", 500*time.Millisecond, "leader heartbeat interval")
}

type AppendEntriesSession struct {
	args  *pb.AppendEntriesReq
	reply *pb.AppendEntriesRes
	tr    trace.Trace
	done  chan bool
	me    int
}

func NewAppendEntriesSession(me int, ctx context.Context, req *pb.AppendEntriesReq, res *pb.AppendEntriesRes) *AppendEntriesSession {
	tr, ok := trace.FromContext(ctx)
	if !ok {
		// tr = trace.New("Raft.AppendEntries", fmt.Sprintf("peer<%d,%d>", me, term))
		glog.Fatal("no trace from context")
	}
	return &AppendEntriesSession{
		args:  req,
		reply: res,
		tr:    tr,
		done:  make(chan bool, 1),
		me:    me,
	}
}

func (session *AppendEntriesSession) trace(format string, arg ...interface{}) {
	session.tr.LazyPrintf(format, arg...)
	if DumpRPCAppendEntries {
		log.Printf("RPCAppendEntries<%d>: %s", session.me, fmt.Sprintf(format, arg...))
	}
}

func (session *AppendEntriesSession) finish() {
	// session.tr.Finish()
}

func (rf *Raft) sendAppendEntries(peer int, req *pb.AppendEntriesReq) (*pb.AppendEntriesRes, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	res, err := rf.peers[peer].AppendEntries(ctx, req)
	if err != nil {
		return res, err
	}
	rf.checkNewTerm(int32(peer), res.Term)
	return res, err
}

func (rf *Raft) AppendEntries(ctx context.Context, req *pb.AppendEntriesReq) (res *pb.AppendEntriesRes, err error) {
	res = &pb.AppendEntriesRes{}
	session := NewAppendEntriesSession(rf.me, ctx, req, res)
	rf.appendEntriesCh <- session
	<-session.done
	session.finish()
	return res, nil
}

func (rf *Raft) processAppendEntries(session *AppendEntriesSession) {
	args := session.args
	reply := session.reply
	rf.checkNewTerm(args.LeaderId, args.Term)
	if len(args.LogEntries) == 0 {
		session.trace("receive heartbeat")
	}

	term := rf.state.getTerm()
	reply.Term = term
	reply.Success = false
	if args.Term < term {
		session.trace("Reject due to term: %d<%d", args.Term, term)
	} else if int(args.PrevLogIndex) <= rf.log.LastIndex() {
		t := rf.log.At(int(args.PrevLogIndex)).Term
		if t == args.PrevLogTerm {
			reply.Success = true
			session.trace("Accept")
			if len(args.LogEntries) > 0 {
				rf.log.AppendAt(int(args.PrevLogIndex)+1, args.LogEntries)
				session.trace("Append log")
			}
		} else {
			session.trace("Reject due to term mismatch at log[%d]: %d!=%d", args.PrevLogIndex, args.PrevLogTerm, t)
		}
	} else {
		session.trace("Reject due to prevLogIndex > lastLogIndex: %d>%d", args.PrevLogIndex, rf.log.LastIndex())
	}

	if reply.Success {
		lastNewEntry := int(args.PrevLogIndex) + len(args.LogEntries)
		if lastNewEntry > rf.state.getCommited() && rf.state.checkFollowerCommit(int(args.LeaderCommit)) {
			session.trace("Follower update commitIndex: commitIndex: %d", rf.state.getCommited())
		}
	}
	session.done <- true
}

// If there's some stuff to replicate to peer, send it, or send a empty heartbeat.
func (rf *Raft) replicateLog(peer int, retreatCnt *int32) {
	// if rf.nextIndex[peer] <= last log Index, send entries until lastLogIndex
	// else send heartbeat, choose empty last log entry to send
	const BatchSize = 100
	peerStr := fmt.Sprintf("peer<%d>", peer)
	rf.state.RLock()
	isHeartBeat := false
	lastIndex := rf.log.LastIndex()
	toReplicate := rf.state.toReplicate(peer)
	prevIndex := toReplicate - 1
	prevTerm := rf.log.At(prevIndex).Term

	args := &pb.AppendEntriesReq{
		Term:         rf.state.CurrentTerm,
		LeaderId:     int32(rf.me),
		PrevLogIndex: int32(prevIndex),
		PrevLogTerm:  prevTerm,
		LeaderCommit: int32(rf.state.CommitIndex),
	}
	if toReplicate <= lastIndex {
		// big step to mix up, small step when retreating
		if *retreatCnt > 0 {
			args.LogEntries = rf.log.Slice(toReplicate, toReplicate+1)
		} else {
			args.LogEntries = rf.log.Slice(toReplicate, toReplicate+BatchSize)
		}
		isHeartBeat = false
	} else {
		isHeartBeat = true
	}
	rf.state.RUnlock()

	res, err := rf.sendAppendEntries(peer, args)
	if err == nil {
		if res.Success {
			*retreatCnt = 0
			if !isHeartBeat {
				rf.state.replicatedToPeer(peer, int(args.PrevLogIndex)+len(args.LogEntries))
				glog.V(utils.VDebug).Infof("%s Replicate to %s succeed, entries range [%d:%d]", rf, peerStr, toReplicate, toReplicate+len(args.LogEntries))
			}
		} else {
			index := rf.state.retreatForPeer(peer)
			*retreatCnt++
			glog.Warningf("%s Replicate to %s failed due to inconsistency, backoff to %d", rf, peerStr, index)
		}
	} else {
		glog.Warningf("%s Replicate to %s failed due to rpc error: %s", rf, peerStr, err)
	}
	return
}
