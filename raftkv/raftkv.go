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
	"github.com/HelloCodeMing/raft-rocks/store"
	"github.com/golang/glog"
	"golang.org/x/net/trace"
)

// flags
var (
	ReportRaftKVState bool
	OpTimeout         time.Duration
	UseRocksDB        bool
	StoragePath       string
	LogStorage        string
)

func init() {
	flag.DurationVar(&OpTimeout, "operation_timeout", 2*time.Second, "operation timeout")
	flag.BoolVar(&ReportRaftKVState, "report_raftkv_state", false, "report raftkv state perioidly")
	flag.BoolVar(&UseRocksDB, "use_rocksdb", false, "use rocksdb dor durable storage")
	flag.StringVar(&StoragePath, "storage_path", "/tmp/raftrocks-storage", "where to store data")
	flag.StringVar(&LogStorage, "log_storage", "rocksdb", "whether memory or rocksdb")
}

// errors
var (
	errNotLeader = errors.New("not leader")
	errTimeout   = errors.New("operation timeout")
)

const (
	clientIDKey = "clientID"
)

// RaftKV consist of: KVStorage, Raft, SessionManager
type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	stopCh  chan bool

	store     store.KVStorage
	persister store.Persister
	waiting   map[int]chan *common.KVCommand

	clientSN map[int64]int64 // client SN should be incremental, otherwise the command will be ignored
	clientID int64           // this field need to be persist
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

func hashCommand(cmd *common.KVCommand) int {
	return int(cmd.ClientID + cmd.SN*1E4)
}

// TODO: wait and notify is too decoupled, may be we could achieve it with a channel?
// Get/PutAppend wait for command applied
// Command could be submit many times, it should be idempotent.
// Command could be identified by <ClientID, SN>
// one command could appear in multi log index, meanwhile on log index could represent multi command, the relationship is N to N.
func (kv *RaftKV) waitFor(cmd *common.KVCommand) error {
	kv.mu.Lock()
	hashVal := hashCommand(cmd)
	kv.waiting[hashVal] = make(chan *common.KVCommand, 1)
	kv.mu.Unlock()
	var err error
	select {
	case res := <-kv.waiting[hashVal]:
		cmd.Res = res.Res
		err = nil
	case <-time.After(OpTimeout):
		err = errTimeout
	}
	kv.mu.Lock()
	delete(kv.waiting, hashVal)
	kv.mu.Unlock()
	return err
}

func (kv *RaftKV) notify(cmd *common.KVCommand) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	hashVal := hashCommand(cmd)
	if ch, ok := kv.waiting[hashVal]; ok {
		ch <- cmd
	} else {
		glog.Warning("Try to finish a non-exist command, it may timeout: ", cmd)
	}
}

func (kv *RaftKV) checkSession(clientID int64) common.ClerkResult {
	if _, ok := kv.clientSN[clientID]; !ok {
		return common.ErrNoSession
	}
	return common.OK
}

func (kv *RaftKV) Get(args *common.GetArgs, reply *common.GetReply) error {
	if kv.checkSession(args.ClientID) != common.OK {
		reply.Status = common.ErrNoSession
		return nil
	}
	cmd := common.NewKVCommand(common.CmdGet, args, reply, args.ClientID, args.SN, args.LogID)
	defer cmd.Finish()
	cmd.Trace("args: %+v", *args)
	err := kv.submitCommand(cmd)
	// TODO: reply is just a pointer in command, command will be serialized and deserialized,
	// so this pointer in resulted cmd will points to different address
	// a work-around for this is to do a deep-copy for this field,
	// but we need a more elegant way
	*reply = *cmd.Res.(*common.GetReply)
	return err
}

func (kv *RaftKV) Put(args *common.PutArgs, reply *common.PutReply) error {
	if kv.checkSession(args.ClientID) != common.OK {
		reply.Status = common.ErrNoSession
		return nil
	}
	cmd := common.NewKVCommand(common.CmdPut, args, reply, args.ClientID, args.SN, args.LogID)
	defer cmd.Finish()
	cmd.Trace("args: %+v", *args)
	return kv.submitCommand(cmd)
}

func (kv *RaftKV) OpenSession(args *common.OpenSessionArgs, reply *common.OpenSessionReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.clientID++
	kv.clientSN[kv.clientID] = 0
	reply.ClientID = kv.clientID
	kv.persister.StoreInt64(clientIDKey, kv.clientID)
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
	glog.Infof("Apply command at index %d to SM", msg.Index)
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

func (kv *RaftKV) doApply() {
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

// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	close(kv.stopCh)
	kv.store.Close()
}

/*
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
*/

// reset state machine from snapshot, callback by raft
func (kv *RaftKV) takeSnapshot(snapshot *raft.Snapshot) {
	glog.Infof("reset state machine from snapshot")
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// kv.db = snapshot.Db
	kv.clientSN = snapshot.ClientSN
}

/*
func initKVStorage() KVStorage {
	if UseRocksDB {
		store, err := MakeRocksDBStore(StoragePath)
		if err != nil {
			glog.Fatal("Fail to create storage,", err)
		}
		return store
	}
	return MakeMemKVStore()
}

// TODO implement memory based log
func initLogStorage() *LogStorage {
	switch LogStorage {
	case "rocksdb":
		opt := gorocksdb.NewDefaultOptions()
		opt.SetCreateIfMissing(true)
		opt.SetCreateIfMissingColumnFamilies(true)
		cfNames := []string{"default", "log", "meta", "kv"}
		db, cfs, err := gorocksdb.OpenDbColumnFamilies(opt, cfNames, []*gorocksdb.Options{opt, opt, opt, opt})
		if err != nil  {
			glog.Fatal("Fail to init Log storage: ", err)
		}
		return MakeLogStorage(db, cfs[1])
	case "memory":
		panic("not implemented")
	default:
		panic("unknow log storage: ", LogStorage)

	}
}
*/

func assertNoError(err error) {
	if err != nil {
		glog.Fatal(err)
	}
}

func (kv *RaftKV) String() string {
	return fmt.Sprintf("RaftKV<%d>", kv.me)
}

// when restart, restore some persistent info
func (kv *RaftKV) restore() {
	clientID, ok := kv.persister.LoadInt64(clientIDKey)
	if ok {
		kv.clientID = clientID
	}
}

func StartRaftKV(servers []*common.ClientEnd, me int) *RaftKV {
	glog.Infoln("Creating RaftKV")
	kv := new(RaftKV)
	kv.me = me

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.stopCh = make(chan bool)
	kv.clientSN = make(map[int64]int64)
	kv.logger = trace.NewEventLog("RaftKV", fmt.Sprintf("peer<%d>", me))
	kv.waiting = make(map[int]chan *common.KVCommand)

	_, columns, err := store.OpenTable(StoragePath, []string{"default", "kv", "log", "meta"})
	assertNoError(err)
	kv.store, err = store.MakeRocksDBStore(columns[1])
	assertNoError(err)
	log, err := store.MakeLogStorage(columns[2])
	assertNoError(err)
	persister := store.MakeRocksBasedPersister(columns[3])
	kv.rf = raft.NewRaft(servers, me, persister, log, kv.applyCh)
	kv.persister = persister
	rpc.Register(kv.rf)

	glog.Infof("Created RaftKV, db: %v, clientDN: %v", kv.store, kv.clientSN)
	go kv.doApply()

	return kv
}
