package raftkv

import (
	"bytes"
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
	waiting  map[*common.KVCommand]chan bool
	clientSN map[int64]int64 // client SN should be incremental, otherwise the command will be ignored
	clientID int64
	logger   trace.EventLog
}

func (kv *RaftKV) doGet(key string) (value string, ok bool) {
	return kv.store.Get(key)
}

func (kv *RaftKV) doPut(key string, value string) {
	kv.store.Put(key, value)
}

func (kv *RaftKV) isUniqueSN(clientID int64, SN int64) bool {
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
func (kv *RaftKV) waitFor(cmd *common.KVCommand) error {
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

func (kv *RaftKV) notify(cmd *common.KVCommand) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if ch, ok := kv.waiting[cmd]; ok {
		ch <- true
	}
}

func (kv *RaftKV) Get(args *common.GetArgs, reply *common.GetReply) error {
	cmd := common.NewKVCommand(common.CmdGet, args, reply, args.ClientID, args.SN, args.LogID)
	defer cmd.Finish()
	cmd.Trace("args: %+v", *args)
	return kv.submitCommand(cmd)
}

func (kv *RaftKV) Put(args *common.PutArgs, reply *common.PutReply) error {
	cmd := common.NewKVCommand(common.CmdPut, args, reply, args.ClientID, args.SN, args.LogID)
	defer cmd.Finish()
	cmd.Trace("args: %+v", *args)
	return kv.submitCommand(cmd)
}

func (kv *RaftKV) OpenSession(args *common.OpenSessionArgs, reply *common.OpenSessionReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.clientID++
	reply.ClientID = kv.clientID
	return nil
}

func (kv *RaftKV) submitCommand(cmd *common.KVCommand) error {
	cmd.Trace("Submit command to raft")
	_, _, isLeader := kv.rf.SubmitCommand(cmd)
	if !isLeader {
		cmd.Trace("peer<%d> not leader, finish this operation", kv.me)
		return errNotLeader
	}
	err := kv.waitFor(cmd)
	return err
}

func (kv *RaftKV) applyCommand(msg raft.ApplyMsg) {
	cmd := msg.Command
	cmd.Trace("Apply start: %+v", cmd)

	if cmd.CmdType != common.CmdGet && !kv.isUniqueSN(cmd.ClientID, cmd.SN) { // ignore duplicate command
		cmd.Trace("Dup command, ignore")
		return
	}
	switch cmd.CmdType {
	case common.CmdGet:
		req := cmd.Req.(*common.GetArgs)
		res := cmd.Res.(*common.GetReply)
		value, ok := kv.doGet(req.Key)
		if ok {
			res.Status = common.OK
			res.Value = value
			cmd.Trace("Apply command, get key=%s,value=%s", req.Key, res.Value)
		} else {
			res.Status = common.ErrNoKey
			cmd.Trace("Apply command, get key=%s,err=%s", req.Key, res.Status)
		}
	case common.CmdPut:
		req := cmd.Req.(*common.PutArgs)
		res := cmd.Res.(*common.PutReply)
		kv.doPut(req.Key, req.Value)
		res.Status = common.OK
		cmd.Trace("Apply command, put key=%s,value=%s", req.Key, req.Value)
	case common.CmdNoop:
	}
	// update read lease
	start := time.Unix(cmd.Timestamp/1E9, cmd.Timestamp%1E9)
	start = start.Add(time.Second)
	// TODO use flag
	kv.rf.UpdateReadLease(msg.Term, start)
}

func (kv *RaftKV) String() string {
	return fmt.Sprintf("RaftKV<%d>", kv.me)
}

func (kv *RaftKV) applier() {
	glog.V(common.VDebug).Infof("%s Applier start", kv.String())
	defer glog.V(common.VDebug).Infof("%s Applier quit", kv.String())

	for {
		select {
		case msg := <-kv.applyCh:
			if msg.UseSnapshot {
				glog.V(common.VDebug).Infof("%s Receive snapshot, lastIncludedIndex=%d", kv.String(), msg.Index)
				s := raft.Snapshot{}
				buf := bytes.NewReader(msg.Snapshot)
				s.Decode(buf)
				kv.takeSnapshot(&s)
			} else {
				glog.V(common.VDump).Infof("%s Receive command: %+v", kv.String(), msg)
				if msg.Command == nil {
					log.Println(kv.rf)
					panic("Should not receive empty command")
				}
				kv.applyCommand(msg)
				kv.notify(msg.Command)
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

func StartRaftKV(servers []*common.ClientEnd, me int, persister *raft.Persister) *RaftKV {
	kv := new(RaftKV)
	kv.me = me

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.stopCh = make(chan bool)
	// TODO inject
	kv.store = MakeMemKVStore()
	kv.clientSN = make(map[int64]int64)
	kv.logger = trace.NewEventLog("RaftKV", fmt.Sprintf("peer<%d>", me))
	kv.persister = persister
	kv.waiting = make(map[*common.KVCommand]chan bool)

	go kv.reporter()
	go kv.applier()

	glog.Infoln("Creating RaftKV")
	kv.rf = raft.NewRaft(servers, me, persister, kv.applyCh)
	rpc.Register(kv.rf)
	// kv.rf.SetSnapshot(kv.snapshot)
	glog.Infof("Created RaftKV, db: %v, clientDN: %v", kv.store, kv.clientSN)

	return kv
}
