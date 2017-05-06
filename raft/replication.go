package raft

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/HelloCodeMing/raft-rocks/pb"
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

func NewAppendEntriesSession(me int, term int32, req *pb.AppendEntriesReq, res *pb.AppendEntriesRes) *AppendEntriesSession {
	tr := trace.New("Raft.AppendEntries", fmt.Sprintf("peer<%d,%d>", me, term))
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
	session.tr.Finish()
}

func (rf *Raft) sendAppendEntries(peer int, req *pb.AppendEntriesReq) (*pb.AppendEntriesRes, error) {
	res, err := rf.peers[peer].AppendEntries(context.TODO(), req)
	if err != nil {
		return res, err
	}
	rf.checkNewTerm(int32(peer), res.Term)
	return res, err
}

func (rf *Raft) AppendEntries(ctx context.Context, req *pb.AppendEntriesReq) (res *pb.AppendEntriesRes, err error) {
	session := NewAppendEntriesSession(rf.me, rf.state.getTerm(), req, res)
	session.trace("args: %+v", *req)
	res.Success = false
	rf.appendEntriesCh <- session
	<-session.done
	session.finish()
	return nil, nil
}

func (rf *Raft) processAppendEntries(session *AppendEntriesSession) {
	args := session.args
	reply := session.reply
	rf.checkNewTerm(args.LeaderId, args.Term)
	if len(args.LogEntries) == 0 {
		session.trace("receive heartbeat")
	}

	if rf.lastIncludedIndex > rf.log.LastIndex() {
		rf.logInfo("state: %v", rf)
		rf.logInfo("args: %+v", args)
		panic(fmt.Sprintf("lastIncludedIndex > lastLogIndex: %d>%d", rf.lastIncludedIndex, rf.log.LastIndex()))
	}
	term := rf.state.getTerm()
	reply.Term = term
	reply.Success = false
	if args.Term < term {
		session.trace("Reject due to term: %d<%d", args.Term, term)
	} else if int(args.PrevLogIndex) <= rf.lastIncludedIndex {
		session.trace("Accept, prevLogIndex cover discarded log")
		reply.Success = true
		offset := rf.lastIncludedIndex - int(args.PrevLogIndex)
		if 0 <= offset && offset < len(args.LogEntries) {
			rf.log.AppendAt(rf.lastIncludedIndex+1, args.LogEntries[offset:])
		}
	} else if int(args.PrevLogIndex) <= rf.log.LastIndex() {
		t := rf.log.At(int(args.PrevLogIndex)).Term
		if t == args.PrevLogTerm {
			session.trace("Accept, append log at %d", args.PrevLogIndex+1)
			reply.Success = true
			rf.log.AppendAt(int(args.PrevLogIndex)+1, args.LogEntries)
		} else {
			session.trace("Reject due to term mismatch at log[%d]: %d!=%d", args.PrevLogIndex, args.PrevLogTerm, t)
		}
	} else {
		session.trace("Reject due to prevLogIndex > lastLogIndex: %d>%d", args.PrevLogIndex, rf.log.LastIndex())
	}

	if reply.Success {
		lastNewEntry := int(args.PrevLogIndex) + len(args.LogEntries)
		if lastNewEntry > rf.state.getCommied() && rf.state.checkFollowerCommit(int(args.LeaderCommit)) {
			session.trace("Follower update commitIndex: commitIndex: %d", rf.state.getCommied())
		}
	}
	session.done <- true
}

// If there's some stuff to replicate to peer, send it, or send a empty heartbeat.
func (rf *Raft) replicateLog(peer int, retreatCnt *int32) {
	// if rf.nextIndex[peer] <= last log Index, send entries until lastLogIndex
	// else send heartbeat, choose empty last log entry to send
	rf.state.RLock()
	isHeartBeat := false
	lastIndex := rf.log.LastIndex()
	toReplicate := rf.state.toReplicate(peer)
	prevIndex := toReplicate - 1
	var prevTerm int32
	if prevIndex <= rf.lastIncludedIndex {
		prevTerm = rf.lastIncludedTerm
	} else {
		prevTerm = rf.log.At(prevIndex).Term
	}

	args := &pb.AppendEntriesReq{
		Term:         rf.state.getTerm(),
		LeaderId:     int32(rf.me),
		PrevLogIndex: int32(prevIndex),
		PrevLogTerm:  prevTerm,
		LeaderCommit: int32(rf.state.getCommied()),
	}
	if toReplicate <= lastIndex {
		// big step to mix up, small step when retreating
		if atomic.LoadInt32(retreatCnt) > 0 {
			args.LogEntries = rf.log.Slice(toReplicate, toReplicate+1)
		} else {
			args.LogEntries = rf.log.Slice(toReplicate, rf.log.LastIndex()+1)
		}
		isHeartBeat = false
	} else {
		isHeartBeat = true
	}
	rf.state.RUnlock()

	res, err := rf.sendAppendEntries(peer, args)
	if err == nil {
		if res.Success {
			*retreatCnt = 1
			atomic.StoreInt32(retreatCnt, 0)
			if !isHeartBeat {
				rf.state.replicatedToPeer(peer, int(args.PrevLogIndex)+len(args.LogEntries))
				rf.logInfo("Replicate entry successfully, {peer:%d, index: %d, entries: %v}", peer, toReplicate, args.LogEntries)
			}
		} else {
			index := rf.state.retreatForPeer(peer)
			*retreatCnt++
			// *retreatCnt *= 2
			// rf.nextIndex[peer] -= int(atomic.LoadInt32(retreatCnt))
			// rf.nextIndex[peer] = maxInt(rf.nextIndex[peer], rf.lastIncludedIndex+1)
			rf.logInfo("Replicate to %d fail due to log inconsistency, retreat to %d", peer, index)
		}
	} else {
		rf.logInfo("Replicate to %d fail due to rpc failure, retry", peer)
	}
	return
}
