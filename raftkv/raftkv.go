package raftkv

import (
	"errors"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/HelloCodeMing/raft-rocks/pb"
	"github.com/HelloCodeMing/raft-rocks/raft"
	"github.com/HelloCodeMing/raft-rocks/store"
	"github.com/HelloCodeMing/raft-rocks/utils"
	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// flags
var (
	OpTimeout   time.Duration
	StoragePath string
)

func init() {
	flag.DurationVar(&OpTimeout, "operation_timeout", 2*time.Second, "operation timeout")
	flag.StringVar(&StoragePath, "storage_path", "/tmp/raftrocks-storage", "where to store data")
}

// errors
var (
	errNotLeader = errors.New("RaftKV not leader")
	errTimeout   = errors.New("RaftKV operation timeout")
)

const (
	clientIDKey = "clientID"
)

// RaftKV consist of: KVStorage, Raft, SessionManager
type RaftKV struct {
	me      int
	rf      *raft.Raft
	applyCh chan *raft.ApplyMsg
	stopCh  chan bool

	store     store.KVStorage
	persister store.Persister

	mu       sync.Mutex // protect waiting and clientSN
	waiting  map[int]chan interface{}
	clientSN map[int64]int64 // client SN should be incremental, otherwise the command will be ignored

	clientID int64 // generate incremental clientID, this field need to be persist
}

// Get rpc interface
// get a value associated with specified key
func (kv *RaftKV) Get(ctx context.Context, req *pb.GetReq) (*pb.GetRes, error) {
	if !kv.checkSession(req.Session) {
		res := &pb.GetRes{Status: pb.Status_NoSession}
		return res, nil
	}
	cmd := &pb.KVCommand{}
	cmd.CmdType = pb.CommandType_Get
	cmd.Command = &pb.KVCommand_GetCommand{GetCommand: req}
	res, err := kv.submitCommand(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return res.(*pb.GetRes), err
}

// Put rpc interface
func (kv *RaftKV) Put(ctx context.Context, req *pb.PutReq) (*pb.PutRes, error) {
	if !kv.checkSession(req.Session) {
		res := &pb.PutRes{Status: pb.Status_NoSession}
		return res, nil
	}
	cmd := &pb.KVCommand{}
	cmd.CmdType = pb.CommandType_Put
	cmd.Command = &pb.KVCommand_PutCommand{PutCommand: req}
	res, err := kv.submitCommand(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return res.(*pb.PutRes), err
}

// OpenSession rpc interface
func (kv *RaftKV) OpenSession(ctx context.Context, req *pb.OpenSessionReq) (*pb.OpenSessionRes, error) {
	if !kv.rf.IsLeader() {
		return nil, errNotLeader
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.clientID++
	kv.clientSN[kv.clientID] = 0
	res := &pb.OpenSessionRes{ClientId: kv.clientID}
	kv.persister.StoreInt64(clientIDKey, kv.clientID)
	return res, nil
}

func (kv *RaftKV) doGet(key string) (value string, ok bool) {
	return kv.store.Get(key)
}

func (kv *RaftKV) doPut(key string, value string) {
	kv.store.Put(key, value)
}

func (kv *RaftKV) isUnique(cmd *pb.KVCommand) bool {
	session := kv.getSession(cmd)
	SN := session.Sn
	if SN <= kv.clientSN[session.ClientId] {
		return false
	}
	kv.mu.Lock()
	kv.clientSN[session.ClientId] = SN
	kv.mu.Unlock()
	return true
}

func (kv *RaftKV) getSession(cmd *pb.KVCommand) *pb.Session {
	var session *pb.Session
	switch cmd.GetCmdType() {
	case pb.CommandType_Get:
		session = cmd.GetGetCommand().Session
	case pb.CommandType_Put:
		session = cmd.GetPutCommand().Session
	case pb.CommandType_Noop:
		panic("not implemented")
	default:
		panic("not implemented")
	}
	return session
}

func (kv *RaftKV) hashCommand(cmd *pb.KVCommand) int {
	session := kv.getSession(cmd)
	return int(session.ClientId + session.Sn*1E4)
}

// TODO: wait and notify is too decoupled, may be we could achieve it with a channel?
// Get/PutAppend wait for command applied
// Command could be submit many times, it should be idempotent.
// Command could be identified by <ClientId, SN>
// one command could appear in multi log index, meanwhile on log index could represent multi command, the relationship is N to N.
func (kv *RaftKV) waitFor(cmd *pb.KVCommand) (out interface{}, err error) {
	hashVal := kv.hashCommand(cmd)
	kv.mu.Lock()
	ch := make(chan interface{}, 1)
	kv.waiting[hashVal] = ch
	kv.mu.Unlock()
	select {
	case res := <-ch:
		out = res
		err = nil
	case <-time.After(OpTimeout):
		err = errTimeout
	}
	kv.mu.Lock()
	delete(kv.waiting, hashVal)
	kv.mu.Unlock()
	return
}

func (kv *RaftKV) notify(cmd *pb.KVCommand, res interface{}) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	hashVal := kv.hashCommand(cmd)
	if ch, ok := kv.waiting[hashVal]; ok {
		ch <- res
	} else {
		glog.Warning("Nobody waiting for this command, ", cmd)
	}
}

func (kv *RaftKV) checkSession(session *pb.Session) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.clientSN[session.ClientId]; !ok {
		return false
	}
	return true
}

func (kv *RaftKV) submitCommand(ctx context.Context, cmd *pb.KVCommand) (interface{}, error) {
	isLeader := kv.rf.SubmitCommand(ctx, cmd)
	if !isLeader {
		return nil, errNotLeader
	}
	return kv.waitFor(cmd)
}

func (kv *RaftKV) applyCommand(msg *raft.ApplyMsg) interface{} {
	cmd := msg.Command
	glog.Infof("%s Apply command at index %d to SM", kv, cmd.Index)
	// cmd.Trace("Apply start: %+v", cmd)

	if cmd.CmdType != pb.CommandType_Get && !kv.isUnique(cmd) { // ignore duplicate command
		// cmd.Trace("Dup command, ignore")
		return nil
	}
	var ret interface{}
	switch cmd.CmdType {
	case pb.CommandType_Get:
		req := cmd.GetGetCommand()
		res := &pb.GetRes{}
		ret = res
		value, ok := kv.doGet(req.Key)
		if ok {
			res.Status = pb.Status_OK
			res.Value = value
			// cmd.Trace("Apply command, get key=%s,value=%s", req.Key, res.Value)
		} else {
			res.Status = pb.Status_NoSuchKey
			// cmd.Trace("Apply command, get key=%s,err=%s", req.Key, res.Status)
		}
	case pb.CommandType_Put:
		req := cmd.GetPutCommand()
		res := &pb.PutRes{}
		ret = res
		kv.doPut(req.Key, req.Value)
		res.Status = pb.Status_OK
		// cmd.Trace("Apply command, put key=%s,value=%s", req.Key, req.Value)
		return res
	case pb.CommandType_Noop:
		panic("not implemented")
	}
	if kv.rf.IsLeader() {
		// update read lease
		start := time.Unix(cmd.Timestamp/1E9, cmd.Timestamp%1E9)
		start = start.Add(time.Second)
		// TODO use flag
		kv.rf.UpdateReadLease(cmd.Term, start)
	}
	return ret
}

func (kv *RaftKV) doApply() {
	glog.V(utils.VDebug).Infof("%s Applier start", kv.String())
	defer glog.V(utils.VDebug).Infof("%s Applier quit", kv.String())

	for {
		select {
		case msg := <-kv.applyCh:
			if msg.UseSnapshot {
				panic("not implemented")
			} else {
				glog.V(utils.VDump).Infof("%s Receive command: %+v", kv.String(), msg)
				if msg.Command == nil {
					glog.Fatalf("Should not receive empty command")
				}
				if kv.rf.IsLeader() {
					res := kv.applyCommand(msg)
					kv.notify(msg.Command, res)
				} else if msg.Command.CmdType == pb.CommandType_Put {
					kv.applyCommand(msg)
				}
			}
		case <-kv.stopCh:
			return
		}
	}
}

func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	close(kv.stopCh)
	kv.store.Close()
}

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

func StartRaftKV(rpcServer *grpc.Server, servers []*utils.ClientEnd, me int) *RaftKV {
	glog.Infoln("Creating RaftKV")
	kv := new(RaftKV)
	kv.me = me

	kv.applyCh = make(chan *raft.ApplyMsg, 100)
	kv.stopCh = make(chan bool)
	kv.clientSN = make(map[int64]int64)
	kv.waiting = make(map[int]chan interface{})

	_, columns, err := store.OpenTable(StoragePath, []string{"default", "kv", "log", "meta"})
	assertNoError(err)
	kv.store, err = store.MakeRocksDBStore(columns[1])
	assertNoError(err)
	log, err := store.MakeLogStorage(columns[2])
	assertNoError(err)
	persister := store.MakeRocksBasedPersister(columns[3])
	kv.rf = raft.NewRaft(servers, me, persister, log, kv.applyCh)
	kv.persister = persister
	pb.RegisterRaftServer(rpcServer, kv.rf)

	glog.Infof("Created RaftKV, db: %v, clientDN: %v", kv.store, kv.clientSN)
	go kv.doApply()

	return kv
}
