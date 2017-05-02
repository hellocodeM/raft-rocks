package raftkv

import (
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"net/rpc"

	"github.com/HelloCodeMing/raft-rocks/common"
	"github.com/HelloCodeMing/raft-rocks/raft"
	"golang.org/x/net/trace"
)

var (
	ReportRaftKVState bool
	DumpRaftKV        bool = false
)

func init() {
	flag.BoolVar(&ReportRaftKVState, "report_raftkv_state", false, "report raftkv state perioidly")
}

type RaftKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	maxRaftState int // snapshot if log grows this big
	applyCh      chan raft.ApplyMsg
	stopCh       chan bool
	persister    *raft.Persister

	db       map[string]string
	waiting  map[*KVCommand]chan bool
	clientSN map[int]int // client SN should be incremental, otherwise the command will be ignored
	logger   trace.EventLog
}

func (kv *RaftKV) logInfo(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	kv.logger.Printf(msg)
	if DumpRaftKV {
		log.Printf("RaftKV<%d>: %s", kv.me, msg)
	}
}

func (kv *RaftKV) doGet(key string) (value string, ok bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok = kv.db[key]
	return
}

func (kv *RaftKV) doPut(key string, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.db[key] = value
}

func (kv *RaftKV) doAppend(key string, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.db[key] += value
}

func (kv *RaftKV) isUniqueSN(clientID int, SN int) bool {
	if SN <= kv.clientSN[clientID] {
		return false
	}
	kv.clientSN[clientID] = SN
	return true
}

func hashCommand(clientID, SN int) int {
	return clientID*1000 + SN
}

// Get/PutAppend wait for command applied
// Command could be submit many times, it should be idempotent.
// Command could be identified by <ClientID, SN>
// one command could appear in multi log index, meanwhile on log index could represent multi command, the relationship is N to N.
func (kv *RaftKV) waitFor(cmd *KVCommand) (err common.ClerkResult) {
	kv.mu.Lock()
	kv.waiting[cmd] = make(chan bool, 1)
	kv.mu.Unlock()
	select {
	case <-kv.waiting[cmd]:
		err = common.OK
	case <-time.After(common.OpTimeout):
		err = common.ErrTimeout
	}
	kv.mu.Lock()
	delete(kv.waiting, cmd)
	kv.mu.Unlock()
	return
}

func (kv *RaftKV) notify(cmd *KVCommand) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if ch, ok := kv.waiting[cmd]; ok {
		ch <- true
	}
}

func (kv *RaftKV) Get(args *common.GetArgs, reply *common.GetReply) {
	cmd := NewKVCommand(CmdGet, args, reply, args.ClientID, args.SN, args.LogID)
	cmd.trace("args: %+v", *args)
	cmd.trace("Submit command to raft")
	_, _, isLeader := kv.rf.SubmitCommand(cmd)
	if !isLeader {
		reply.Err = common.ErrNotLeader
		cmd.trace("peer<%d> not leader, finish this operation", kv.me)
		cmd.tracer.Finish()
		return
	}
	reply.Err = kv.waitFor(cmd)
	cmd.trace("reply: %+v", reply)
	cmd.tracer.Finish()
}

func (kv *RaftKV) PutAppend(args *common.PutAppendArgs, reply *common.PutAppendReply) {
	var cmd *KVCommand
	if args.IsAppend {
		cmd = NewKVCommand(CmdAppend, args, reply, args.ClientID, args.SN, args.LogID)
	} else {
		cmd = NewKVCommand(CmdPut, args, reply, args.ClientID, args.SN, args.LogID)
	}
	cmd.trace("args: %+v", *args)
	cmd.trace("Submit command to raft")
	_, _, isLeader := kv.rf.SubmitCommand(cmd)
	if !isLeader {
		reply.Err = common.ErrNotLeader
		cmd.trace("peer<%d> not leader, finish this operation", kv.me)
		cmd.tracer.Finish()
		return
	}
	reply.Err = kv.waitFor(cmd)
	cmd.trace("reply: %+v", *reply)
	cmd.tracer.Finish()
}

func (kv *RaftKV) applyCommand(cmd *KVCommand) {
	cmd.trace("Apply start: %+v", cmd)

	if cmd.CmdType != CmdGet && !kv.isUniqueSN(cmd.ClientID, cmd.SN) { // ignore duplicate command
		cmd.trace("Dup command, ignore")
		return
	}
	switch cmd.CmdType {
	case CmdGet:
		req := cmd.Req.(*common.GetArgs)
		res := cmd.Res.(*common.GetReply)
		value, ok := kv.doGet(req.Key)
		if ok {
			res.Err = common.OK
			res.Value = value
		} else {
			res.Err = common.ErrNoKey
		}
		cmd.trace("doGet: {k:%s, v:%s}", req.Key, res.Value)
	case CmdPut:
		req := cmd.Req.(*common.PutAppendArgs)
		res := cmd.Res.(*common.PutAppendReply)
		kv.doPut(req.Key, req.Value)
		res.Err = common.OK
		cmd.trace("doPut: {k:%s, v:%s}", req.Key, req.Value)
	case CmdAppend:
		cmd.trace("Do append")
		req := cmd.Req.(*common.PutAppendArgs)
		res := cmd.Res.(*common.PutAppendReply)
		kv.doAppend(req.Key, req.Value)
		res.Err = common.OK
		cmd.trace("doAppend: {k:%s, v:%s}", req.Key, req.Value)
	}
}

func (kv *RaftKV) applier() {
	kv.logInfo("Applier start")
	defer kv.logInfo("Applier quit")

	for {
		select {
		case msg := <-kv.applyCh:
			if msg.UseSnapshot {
				kv.logInfo("Receive snapshot, lastIncludedIndex=%d", msg.Index)
				s := raft.Snapshot{}
				buf := bytes.NewReader(msg.Snapshot)
				s.Decode(buf)
				kv.takeSnapshot(&s)
			} else {
				kv.logInfo("Receive command: %+v", msg)
				if msg.Command == nil {
					log.Println(kv.rf)
					log.Println(kv.db)
					panic("Should not receive empty command")
				}
				cmd := msg.Command.(*KVCommand)
				kv.applyCommand(cmd)
				kv.notify(cmd)
			}
		case <-kv.stopCh:
			return
		}
	}
}

func (kv *RaftKV) reporter() {
	if !ReportRaftKVState {
		return
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			kv.logInfo("KV State: {db: %v}", kv.db)
		case <-kv.stopCh:
			return
		}
	}
}

// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	close(kv.stopCh)
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*common.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(&KVCommand{})
	gob.Register(&common.PutAppendArgs{})
	gob.Register(&common.PutAppendReply{})
	gob.Register(&common.GetArgs{})
	gob.Register(&common.GetReply{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxRaftState = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.stopCh = make(chan bool)
	kv.db = make(map[string]string)
	kv.clientSN = make(map[int]int)
	kv.logger = trace.NewEventLog("RaftKV", fmt.Sprintf("peer<%d>", me))
	kv.persister = persister
	kv.waiting = make(map[*KVCommand]chan bool)

	go kv.reporter()
	go kv.applier()

	kv.logInfo("Creating RaftKV")
	kv.rf = raft.NewRaft(servers, me, persister, kv.applyCh)
	rpc.Register(kv.rf)
	kv.rf.SetSnapshot(kv.snapshot)
	kv.rf.SetMaxStateSize(maxraftstate)
	kv.logInfo("Created RaftKV, db: %v, clientDN: %v", kv.db, kv.clientSN)

	return kv
}

// snapshot state to persister, callback by raft
func (kv *RaftKV) snapshot(lastIndex int, lastTerm int32) {
	kv.logInfo("snapshot state to persister")
	kv.mu.Lock()
	defer kv.mu.Unlock()
	buf := new(bytes.Buffer)
	s := raft.Snapshot{
		LastIncludedIndex: lastIndex,
		LastIncludedTerm:  lastTerm,
		Db:                kv.db,
		ClientSN:          kv.clientSN,
	}
	s.Encode(buf)
	kv.persister.SaveSnapshot(buf.Bytes())
}

// reset state machine from snapshot, callback by raft
func (kv *RaftKV) takeSnapshot(snapshot *raft.Snapshot) {
	kv.logInfo("reset state machine from snapshot")
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.db = snapshot.Db
	kv.clientSN = snapshot.ClientSN
}
