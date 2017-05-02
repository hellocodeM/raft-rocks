package raftkv

import (
	"bytes"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"sync"
	"time"

	"github.com/HelloCodeMing/raft-rocks/common"
	"github.com/HelloCodeMing/raft-rocks/raft"
	"github.com/golang/glog"
	"golang.org/x/net/trace"
)

// flags
var (
	ReportRaftKVState bool
	OpTimeout         time.Duration
)

func init() {
	flag.DurationVar(&OpTimeout, "operation_timeout", 2*time.Second, "operation timeout")
	flag.BoolVar(&ReportRaftKVState, "report_raftkv_state", false, "report raftkv state perioidly")
}

// errors
var (
	errNotLeader = errors.New("not leader")
	errTimeout   = errors.New("operation timeout")
)

type RaftKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	maxRaftState int // snapshot if log grows this big
	applyCh      chan raft.ApplyMsg
	stopCh       chan bool
	persister    *raft.Persister

	store    KVStorage
	waiting  map[*KVCommand]chan bool
	clientSN map[int]int // client SN should be incremental, otherwise the command will be ignored
	logger   trace.EventLog
}

func (kv *RaftKV) doGet(key string) (value string, ok bool) {
	return kv.store.Get(key)
}

func (kv *RaftKV) doPut(key string, value string) {
	kv.store.Put(key, value)
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
func (kv *RaftKV) waitFor(cmd *KVCommand) error {
	kv.mu.Lock()
	kv.waiting[cmd] = make(chan bool, 1)
	kv.mu.Unlock()
	var err error
	select {
	case <-kv.waiting[cmd]:
		err = nil
	case <-time.After(OpTimeout):
		err = errTimeout
	}
	kv.mu.Lock()
	delete(kv.waiting, cmd)
	kv.mu.Unlock()
	return err
}

func (kv *RaftKV) notify(cmd *KVCommand) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if ch, ok := kv.waiting[cmd]; ok {
		ch <- true
	}
}

func (kv *RaftKV) Get(args *common.GetArgs, reply *common.GetReply) error {
	cmd := NewKVCommand(CmdGet, args, reply, args.ClientID, args.SN, args.LogID)
	cmd.trace("args: %+v", *args)
	return kv.submitCommand(cmd)
}

func (kv *RaftKV) Put(args *common.PutArgs, reply *common.PutReply) error {
	cmd := NewKVCommand(CmdPut, args, reply, args.ClientID, args.SN, args.LogID)
	cmd.trace("args: %+v", *args)
	return kv.submitCommand(cmd)
}

func (kv *RaftKV) submitCommand(cmd *KVCommand) error {
	cmd.trace("Submit command to raft")
	_, _, isLeader := kv.rf.SubmitCommand(cmd)
	if !isLeader {
		cmd.trace("peer<%d> not leader, finish this operation", kv.me)
		cmd.tracer.Finish()
		return errNotLeader
	}
	err := kv.waitFor(cmd)
	cmd.tracer.Finish()
	return err
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
		req := cmd.Req.(*common.PutArgs)
		res := cmd.Res.(*common.PutReply)
		kv.doPut(req.Key, req.Value)
		res.Err = common.OK
		cmd.trace("doPut: {k:%s, v:%s}", req.Key, req.Value)
	}
}

func (kv *RaftKV) applier() {
	glog.V(common.VDebug).Infoln("Applier start")
	defer glog.V(common.VDebug).Infoln("Applier quit")

	for {
		select {
		case msg := <-kv.applyCh:
			if msg.UseSnapshot {
				glog.V(common.VDebug).Infof("Receive snapshot, lastIncludedIndex=%d", msg.Index)
				s := raft.Snapshot{}
				buf := bytes.NewReader(msg.Snapshot)
				s.Decode(buf)
				kv.takeSnapshot(&s)
			} else {
				glog.V(common.VDump).Infof("Receive command: %+v", msg)
				if msg.Command == nil {
					log.Println(kv.rf)
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
			glog.V(common.VDump).Infof("KV State: {db: %v}", kv.store)
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
	gob.Register(&common.PutArgs{})
	gob.Register(&common.PutReply{})
	gob.Register(&common.GetArgs{})
	gob.Register(&common.GetReply{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxRaftState = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.stopCh = make(chan bool)
	// TODO inject
	kv.store = MakeMemKVStore()
	kv.clientSN = make(map[int]int)
	kv.logger = trace.NewEventLog("RaftKV", fmt.Sprintf("peer<%d>", me))
	kv.persister = persister
	kv.waiting = make(map[*KVCommand]chan bool)

	go kv.reporter()
	go kv.applier()

	glog.Infoln("Creating RaftKV")
	kv.rf = raft.NewRaft(servers, me, persister, kv.applyCh)
	rpc.Register(kv.rf)
	kv.rf.SetSnapshot(kv.snapshot)
	kv.rf.SetMaxStateSize(maxraftstate)
	glog.Infof("Created RaftKV, db: %v, clientDN: %v", kv.store, kv.clientSN)

	return kv
}

// snapshot state to persister, callback by raft
func (kv *RaftKV) snapshot(lastIndex int, lastTerm int32) {
	glog.Infof("snapshot state to persister")
	kv.mu.Lock()
	defer kv.mu.Unlock()
	buf := new(bytes.Buffer)
	// TODO: snapshot
	s := raft.Snapshot{
		LastIncludedIndex: lastIndex,
		LastIncludedTerm:  lastTerm,
		// Db:                kv.db,
		ClientSN: kv.clientSN,
	}
	s.Encode(buf)
	kv.persister.SaveSnapshot(buf.Bytes())
}

// reset state machine from snapshot, callback by raft
func (kv *RaftKV) takeSnapshot(snapshot *raft.Snapshot) {
	glog.Infof("reset state machine from snapshot")
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// kv.db = snapshot.Db
	kv.clientSN = snapshot.ClientSN
}
