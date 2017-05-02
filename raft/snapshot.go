package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"time"

	"golang.org/x/net/trace"
)

const DumpRPCInstallSnapshotTracing = true

type Snapshot struct {
	Db                map[string]string
	ClientSN          map[int]int
	LastIncludedIndex int
	LastIncludedTerm  int32
}

func (s *Snapshot) Encode(writer io.Writer) {
	encoder := gob.NewEncoder(writer)
	encoder.Encode(s.LastIncludedIndex)
	encoder.Encode(s.LastIncludedTerm)
	encoder.Encode(s.Db)
	encoder.Encode(s.ClientSN)
}

func (s *Snapshot) Decode(reader io.Reader) {
	decoder := gob.NewDecoder(reader)
	decoder.Decode(&s.LastIncludedIndex)
	decoder.Decode(&s.LastIncludedTerm)
	decoder.Decode(&s.Db)
	decoder.Decode(&s.ClientSN)
}

type InstallSnapshotArgs struct {
	Term              int32
	LeaderID          int32
	LastIncludedIndex int
	LastIncludedTerm  int32

	Offset int
	Chunk  []byte
	Done   bool
}

type InstallSnapshotReply struct {
	Term    int32
	Success bool
}

type installSnapshotSession struct {
	args  *InstallSnapshotArgs
	reply *InstallSnapshotReply
	tr    trace.Trace
}

func (session *installSnapshotSession) trace(format string, a ...interface{}) {
	session.tr.LazyPrintf(format, a...)
	if DumpRPCInstallSnapshotTracing {
		log.Printf("RPCInstallSnapshot: %s", fmt.Sprintf(format, a...))
	}
}

func (rf *Raft) sendInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
	rf.checkNewTerm(int32(peer), reply.Term)
	return ok
}

func (rf *Raft) notifySMTakeSnapshot() {
	if rf.lastIncludedIndex == 0 {
		return
	}
	rf.logInfo("Notify state machine to take snapshot: {lastIncludedIndex: %d}", rf.lastIncludedIndex)
	msg := ApplyMsg{
		Index:       rf.lastIncludedIndex,
		Command:     nil,
		UseSnapshot: true,
		Snapshot:    rf.persister.ReadSnapshot(),
	}
	rf.applyCh <- msg
	rf.lastApplied = rf.lastIncludedIndex
}

// InstallSnapshot raft InstallSnapshot RPC
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// prepare
	tr := trace.New("Raft.InstallSnapshot", fmt.Sprintf("peer<%d>", rf.me))
	defer tr.Finish()
	session := installSnapshotSession{
		args:  args,
		reply: reply,
		tr:    tr,
	}
	session.trace(
		"Args: {Term: %d, LeaderID: %d, LastIncludedIndex: %d, LastIncludedTerm: %d}",
		args.Term, args.LeaderID, args.LastIncludedIndex, args.LastIncludedTerm)
	defer session.trace("reply: %+v", reply)

	// validation
	rf.checkNewTerm(args.LeaderID, args.Term)
	rf.Lock()
	defer rf.Unlock()
	{
		reply.Success = false
		reply.Term = rf.currentTerm
		if args.Term < rf.currentTerm {
			session.trace("Reject due to term %d < %d", args.Term, rf.currentTerm)
			return
		}
		if args.LastIncludedIndex <= rf.lastIncludedIndex {
			session.trace("Reject due to earlier snapshot: %d < %d", args.LastIncludedIndex, rf.lastIncludedIndex)
			return
		}
		logOK := args.LastIncludedIndex > rf.log.LastIndex() || rf.log.At(args.LastIncludedIndex).Term == args.LastIncludedTerm
		if !logOK {
			session.trace("Reject due to Log mismatch: %d != %d", rf.log.At(args.LastIncludedIndex).Term, args.LastIncludedTerm)
			return
		}
	}
	// write data to snapshot file
	// wait for all data if done if false
	// retain log entry following the snapshot
	// discard log
	// reset state machine using snapshot
	if args.Done {
		session.trace("Before snapshot raft state size: %d", rf.persister.RaftStateSize())
		reply.Success = true
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.persister.SaveSnapshot(args.Chunk)
		rf.notifySMTakeSnapshot()
		rf.log.DiscardUntil(args.LastIncludedIndex)
		rf.commitIndex = maxInt(args.LastIncludedIndex, rf.commitIndex)
		rf.persist()

		session.trace("After snapshot state become: {lastApplied: %d, commitIndex: %d}", rf.lastApplied, rf.commitIndex)
		session.trace("After snapshot raft state size: %d", rf.persister.RaftStateSize())

		// check
		for i := rf.lastApplied + 1; i < rf.log.Length(); i++ {
			if rf.log.At(i).Command == nil {
				log.Println(rf)
				log.Fatalf("After snapshot log should not contains empty command")
			}
		}
	}
}

func (rf *Raft) sendSnapshotTo(peer int) (fastRetry bool) {
	rf.logInfo("Sending snapshot to peer<%d>, {lastIncludedIndex: %d, lastIncludedTerm: %d}",
		peer, rf.lastIncludedIndex, rf.lastIncludedTerm)
	rf.RLock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          int32(rf.me),
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Offset:            0,
		Chunk:             rf.persister.ReadSnapshot(),
		Done:              true,
	}
	rf.RUnlock()
	reply := InstallSnapshotReply{}
	if rf.sendInstallSnapshot(peer, &args, &reply) && reply.Success {
		rf.Lock()
		rf.matchIndex[peer] = maxInt(rf.lastIncludedIndex, rf.matchIndex[peer])
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		rf.Unlock()
		rf.logInfo("Update peer<%d> state: {matchIndex: %d, nextIndex: %d}", peer, rf.matchIndex[peer], rf.nextIndex[peer])
	}
	return false
}

func (rf *Raft) snapshoter() {
	rf.logInfo("Snapshoter start")
	defer rf.logInfo("Snapshoter quit")
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-rf.shutdownCh:
			return
		case <-ticker.C:
			rf.checkIfNeededSnapshot()
		}
	}
}

func (rf *Raft) checkIfNeededSnapshot() {
	if rf.maxStateSize == 0 || rf.maxStateSize == -1 || rf.snapshotCB == nil {
		return
	}
	rf.Lock()
	defer rf.Unlock()
	if rf.persister.RaftStateSize() >= rf.maxStateSize && rf.lastApplied > rf.lastIncludedIndex {
		rf.logInfo("CheckIfNeededSnapshot: raft state too large %d > %d", rf.persister.RaftStateSize(), rf.maxStateSize)
		rf.lastIncludedIndex = rf.lastApplied
		rf.lastIncludedTerm = rf.log.At(rf.lastApplied).Term
		rf.snapshotCB(rf.lastIncludedIndex, rf.lastIncludedTerm)
		rf.log.DiscardUntil(rf.lastIncludedIndex)
		rf.persist()
		if rf.role == leader {
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = maxInt(rf.nextIndex[i], rf.lastIncludedIndex+1)
			}
		}
		rf.logInfo("Snapshot state and discard log: {lastIncludedIndex: %d, lastIncludedTerm: %d, nextIndex: %v}",
			rf.lastIncludedIndex, rf.lastIncludedTerm, rf.nextIndex)
	}
}

func (rf *Raft) recoverSnapshot() {
	rf.Lock()
	defer rf.Unlock()
	buff := bytes.NewReader(rf.persister.ReadSnapshot())
	s := Snapshot{}
	s.Decode(buff)
	rf.lastIncludedIndex = s.LastIncludedIndex
	rf.lastIncludedTerm = s.LastIncludedTerm
	rf.commitIndex = maxInt(rf.commitIndex, rf.lastIncludedIndex)
	if rf.lastIncludedIndex > 0 {
		rf.log.DiscardUntil(rf.lastIncludedIndex)
		rf.persist()
		rf.notifySMTakeSnapshot()
	}
}
