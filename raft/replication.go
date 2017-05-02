package raft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"flag"

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
	rf.checkApply()
	return err == nil
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	session := NewAppendEntriesSession(rf.me, rf.currentTerm, args, reply)
	session.trace("args: %+v", *args)
	// session.trace("raft state: %+v", rf)
	reply.Success = false
	rf.appendEntriesCh <- session
	<-session.done
	reply.Term = rf.currentTerm
	session.finish()
}

func (rf *Raft) processAppendEntries(session *AppendEntriesSession) {
	args := session.args
	reply := session.reply
	rf.checkNewTerm(args.LeaderID, args.Term)
	if len(args.Entries) == 0 {
		session.trace("receive heartbeat")
	}

	rf.Lock()
	if rf.lastIncludedIndex > rf.log.LastIndex() {
		rf.Unlock()
		rf.logInfo("state: %v", rf)
		rf.logInfo("args: %+v", args)
		panic(fmt.Sprintf("lastIncludedIndex > lastLogIndex: %d>%d", rf.lastIncludedIndex, rf.log.LastIndex()))
	}
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		session.trace("Reject due to term: %d<%d", args.Term, rf.currentTerm)
	} else if args.PrevLogIndex <= rf.lastIncludedIndex {
		session.trace("Accept, prevLogIndex cover discarded log")
		reply.Success = true
		offset := rf.lastIncludedIndex - args.PrevLogIndex
		if 0 <= offset && offset < len(args.Entries) {
			rf.log.AppendAt(rf.lastIncludedIndex+1, args.Entries[offset:])
			rf.persist()
		}
	} else if args.PrevLogIndex <= rf.log.LastIndex() {
		t := rf.log.At(args.PrevLogIndex).Term
		if t == args.PrevLogTerm {
			session.trace("Accept, append log at %d", args.PrevLogIndex+1)
			reply.Success = true
			rf.log.AppendAt(args.PrevLogIndex+1, args.Entries)
			rf.persist()
		} else {
			session.trace("Reject due to term mismatch at log[%d]: %d!=%d", args.PrevLogIndex, args.PrevLogTerm, t)
		}
	} else {
		session.trace("Reject due to prevLogIndex > lastLogIndex: %d>%d", args.PrevLogIndex, rf.log.LastIndex())
	}

	if reply.Success {
		lastNewEntry := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit > rf.commitIndex && lastNewEntry > rf.commitIndex {
			rf.commitIndex = minInt(args.LeaderCommit, lastNewEntry)
			session.trace("Follower update commitIndex: commitIndex: %d", rf.commitIndex)
		}
	}
	rf.Unlock()
	rf.checkApply()
	session.done <- true
}

// If there's some stuff to replicate to peer, send it, or send a empty heartbeat.
func (rf *Raft) replicateLog(peer int, retreatCnt *int32) {
	// if rf.nextIndex[peer] <= last log Index, send entries until lastLogIndex
	// else send heartbeat, choose empty last log entry to send
	rf.RLock()
	isHeartBeat := false
	lastIndex := rf.log.LastIndex()
	index := rf.nextIndex[peer]
	prevIndex := index - 1
	var prevTerm int32
	if prevIndex <= rf.lastIncludedIndex {
		prevTerm = rf.lastIncludedTerm
	} else {
		prevTerm = rf.log.At(prevIndex).Term
	}

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     int32(rf.me),
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		LeaderCommit: rf.commitIndex,
	}
	if rf.nextIndex[peer] <= lastIndex {
		// big step to mix up, small step when retreating
		if atomic.LoadInt32(retreatCnt) > 0 {
			args.Entries = rf.log.Slice(index, index+1)
		} else {
			args.Entries = rf.log.Slice(index, rf.log.Length())
		}
		isHeartBeat = false
	} else {
		isHeartBeat = true
	}
	rf.RUnlock()

	reply := new(AppendEntriesReply)
	ok := rf.sendAppendEntries(peer, args, reply)
	if ok {
		rf.Lock()
		if reply.Success {
			*retreatCnt = 1
			atomic.StoreInt32(retreatCnt, 0)
			if !isHeartBeat {
				rf.matchIndex[peer] = maxInt(args.PrevLogIndex+len(args.Entries), rf.matchIndex[peer])
				rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
				rf.logInfo("Replicate entry successfully, {peer:%d, index: %d, entries: %v}", peer, index, args.Entries)
			}
		} else {
			rf.retreatForPeer(peer)
			*retreatCnt++
			// *retreatCnt *= 2
			// rf.nextIndex[peer] -= int(atomic.LoadInt32(retreatCnt))
			// rf.nextIndex[peer] = maxInt(rf.nextIndex[peer], rf.lastIncludedIndex+1)
			rf.logInfo("Replicate to %d fail due to log inconsistency, retreat to %d", peer, rf.nextIndex[peer])
		}
		rf.Unlock()
	} else {
		rf.logInfo("Replicate to %d fail due to rpc failure, retry", peer)
	}
	return
}

func (rf *Raft) retreatForPeer(peer int) {
	idx := &rf.nextIndex[peer]
	*idx = minInt(*idx, rf.log.LastIndex())
	if *idx > rf.lastIncludedIndex {
		oldTerm := rf.log.At(*idx).Term
		for *idx > rf.lastIncludedIndex+1 && rf.log.At(*idx).Term == oldTerm {
			*idx--
		}
	}
	*idx = maxInt(*idx, rf.lastIncludedIndex+1)
}

// Replicate command to all peers.
func (rf *Raft) replicator(peer int, quitCh <-chan bool, done *sync.WaitGroup) {
	defer done.Done()
	rf.logInfo("Replicator start for peer %d", peer)
	defer rf.logInfo("Replicator for peer %d quit", peer)

	const retreatLimit = 4
	var retreatCnt int32 = 1 // for fast retreat

	// initial heartbeat
	go rf.replicateLog(peer, &retreatCnt)
	for {
		// 1. no submit, periodically check if exists log to replicate
		// 2. exists submit, replicateLog
		select {
		case <-quitCh:
			return
		case <-time.After(time.Duration(heartbeatTO)):
		case <-rf.submitCh:
		}

		go func() {
			// when to send snapshot
			// 1. retreat too many times
			// 2. nextIndex reach lastIncluded
			snapshot := rf.lastIncludedIndex > 0 &&
				(atomic.LoadInt32(&retreatCnt) > retreatLimit || rf.nextIndex[peer] <= rf.lastIncludedIndex)
			if snapshot {
				rf.sendSnapshotTo(peer)
				atomic.StoreInt32(&retreatCnt, 1)
			} else {
				rf.replicateLog(peer, &retreatCnt)
			}
			rf.commitCh <- true
		}()
	}
}
