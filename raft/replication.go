package raft

import (
	"flag"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"golang.org/x/net/trace"
)

const DumpRPCAppendEntries = false

var (
	heartbeatTO time.Duration
)

func init() {
	flag.DurationVar(&heartbeatTO, "leader_heartbeat", 500*time.Millisecond, "leader heartbeat interval")
}

type AppendEntriesArgs struct {
	Term         int32      // leader's term
	LeaderID     int32      // so follower can redirect clients
	PrevLogIndex int        // index of log entry preceding new one
	PrevLogTerm  int32      // term of prevLogIndex entry
	Entries      []LogEntry // entries to store, empty for heartbeat
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int32 // currentTerm, for leader to update itself
	Success bool  // true if follower contained entry matching prevLogIndex and prevLogTerm
}

type AppendEntriesSession struct {
	args  *AppendEntriesArgs
	reply *AppendEntriesReply
	tr    trace.Trace
	done  chan bool
	me    int
}

func NewAppendEntriesSession(me int, term int32, args *AppendEntriesArgs, reply *AppendEntriesReply) *AppendEntriesSession {
	tr := trace.New("Raft.AppendEntries", fmt.Sprintf("peer<%d,%d>", me, term))
	return &AppendEntriesSession{
		args:  args,
		reply: reply,
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

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	err := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	rf.checkNewTerm(int32(peer), reply.Term)
	return err == nil
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	session := NewAppendEntriesSession(rf.me, rf.state.getTerm(), args, reply)
	session.trace("args: %+v", *args)
	reply.Success = false
	rf.appendEntriesCh <- session
	<-session.done
	session.finish()
	return nil
}

func (rf *Raft) processAppendEntries(session *AppendEntriesSession) {
	args := session.args
	reply := session.reply
	rf.checkNewTerm(args.LeaderID, args.Term)
	if len(args.Entries) == 0 {
		session.trace("receive heartbeat")
	}

	rf.state.Lock()
	defer rf.state.Unlock()
	if rf.lastIncludedIndex > rf.log.LastIndex() {
		rf.logInfo("state: %v", rf)
		rf.logInfo("args: %+v", args)
		panic(fmt.Sprintf("lastIncludedIndex > lastLogIndex: %d>%d", rf.lastIncludedIndex, rf.log.LastIndex()))
	}
	term := rf.state.currentTerm
	reply.Term = term
	reply.Success = false
	if args.Term < term {
		session.trace("Reject due to term: %d<%d", args.Term, term)
	} else if args.PrevLogIndex <= rf.lastIncludedIndex {
		session.trace("Accept, prevLogIndex cover discarded log")
		reply.Success = true
		offset := rf.lastIncludedIndex - args.PrevLogIndex
		if 0 <= offset && offset < len(args.Entries) {
			rf.log.AppendAt(rf.lastIncludedIndex+1, args.Entries[offset:])
			rf.state.persist()
		}
	} else if args.PrevLogIndex <= rf.log.LastIndex() {
		t := rf.log.At(args.PrevLogIndex).Term
		if t == args.PrevLogTerm {
			session.trace("Accept, append log at %d", args.PrevLogIndex+1)
			reply.Success = true
			rf.log.AppendAt(args.PrevLogIndex+1, args.Entries)
			rf.state.persist()
		} else {
			session.trace("Reject due to term mismatch at log[%d]: %d!=%d", args.PrevLogIndex, args.PrevLogTerm, t)
		}
	} else {
		session.trace("Reject due to prevLogIndex > lastLogIndex: %d>%d", args.PrevLogIndex, rf.log.LastIndex())
	}

	if reply.Success {
		lastNewEntry := args.PrevLogIndex + len(args.Entries)
		if lastNewEntry > rf.state.getCommied() && rf.state.checkFollowerCommit(args.LeaderCommit) {
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

	args := &AppendEntriesArgs{
		Term:         rf.state.getTerm(),
		LeaderID:     int32(rf.me),
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		LeaderCommit: rf.state.getCommied(),
	}
	if toReplicate <= lastIndex {
		// big step to mix up, small step when retreating
		if atomic.LoadInt32(retreatCnt) > 0 {
			args.Entries = rf.log.Slice(toReplicate, toReplicate+1)
		} else {
			args.Entries = rf.log.Slice(toReplicate, rf.log.Length())
		}
		isHeartBeat = false
	} else {
		isHeartBeat = true
	}
	rf.state.RUnlock()

	reply := new(AppendEntriesReply)
	ok := rf.sendAppendEntries(peer, args, reply)
	if ok {
		if reply.Success {
			*retreatCnt = 1
			atomic.StoreInt32(retreatCnt, 0)
			if !isHeartBeat {
				rf.state.replicatedToPeer(peer, args.PrevLogIndex+len(args.Entries))
				rf.logInfo("Replicate entry successfully, {peer:%d, index: %d, entries: %v}", peer, toReplicate, args.Entries)
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
