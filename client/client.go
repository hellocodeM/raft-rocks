package main

import (
	"flag"
	"fmt"
	"net/rpc"
	"strings"
	"time"

	"github.com/HelloCodeMing/raft-rocks/common"
	"github.com/golang/glog"
)

var (
	ServerAddrs   string
	RetryInterval time.Duration

	// client actions
	Putting  bool
	Getting  bool
	Key      string
	Value    string
	ClientID int
)

func init() {
	flag.StringVar(&ServerAddrs, "server_addrs", "localhost:10000,localhost:10001,localhost:10002", "server addresses")
	flag.DurationVar(&RetryInterval, "retry_interval", time.Second, "interval retry a failed server")
	flag.BoolVar(&Putting, "put", false, "put a key/value")
	flag.BoolVar(&Getting, "get", false, "get a key/value")
	flag.StringVar(&Key, "key", "", "key")
	flag.StringVar(&Value, "value", "", "value")
	flag.IntVar(&ClientID, "client_id", 0, "client id")
}

// Clerk client implementation
type Clerk struct {
	serverAddrs []string
	leader      int

	clientID int
	SN       int
}

// MakeClerk create a client
func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.serverAddrs = servers
	ck.clientID = ClientID
	ck.logInfo("Created clerk, clientID:%d", ck.clientID)
	return ck
}

func (ck *Clerk) logInfo(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	glog.Infof(msg)
}

// Get get a key/value
func (ck *Clerk) Get(key string) (string, bool) {
	logID := common.GenLogID()
	ck.SN++
	ck.logInfo("%s Geting: {k:%s}", logID, key)
	args := &common.GetArgs{Key: key, ClientID: ck.clientID, SN: ck.SN, LogID: logID}
	reply := &common.GetReply{}
	for r := 0; r < 10; r++ {
		client, err := rpc.DialHTTP("tcp", ck.serverAddrs[ck.leader])
		if err != nil {
			glog.Warningf("Dial %s failed", ck.serverAddrs[ck.leader])
			ck.rollLeader()
			continue
		}
		err = client.Call("RaftKV.Get", &args, reply)
		if err == nil && reply.Err == common.OK {
			return reply.Value, true
		}
		ck.handleError(err, reply.Err)
	}

	return "", false
}

func (ck *Clerk) PutAppend(logID string, key string, value string, isAppend bool) {
	ck.SN++
	args := common.PutArgs{
		Key:      key,
		Value:    value,
		SN:       ck.SN,
		ClientID: ck.clientID,
		LogID:    logID,
	}
	for r := 0; r < 10; r++ {
		reply := &common.PutReply{}
		client, err := rpc.DialHTTP("tcp", ck.serverAddrs[ck.leader])
		if err != nil {
			glog.Warningf("Dial %s failed", ck.serverAddrs[ck.leader])
			ck.rollLeader()
			continue
		}
		err = client.Call("RaftKV.Put", &args, reply)
		if err == nil && reply.Err == common.OK {
			return
		}
		ck.handleError(err, reply.Err)
	}
}

func (ck *Clerk) handleError(rpcErr error, err common.ClerkResult) {
	if rpcErr != nil {
		ck.logInfo("rpc error, try server: %d", ck.leader)
	} else {
		switch err {
		case common.ErrNotLeader:
			ck.logInfo("wrong leader")
		case common.ErrTimeout:
			ck.logInfo("operation timeout, maybe network partition")
		case common.ErrNoKey:
			ck.logInfo("no such key, maybe stale data")
		default:
			ck.logInfo("error is %s", err)
		}
	}
	ck.rollLeader()
	ck.logInfo("try server: %d", ck.leader)
	time.Sleep(RetryInterval)
}

func (ck *Clerk) rollLeader() {
	ck.leader = (ck.leader + 1) % len(ck.serverAddrs)
}

func (ck *Clerk) Put(key string, value string) {
	logID := common.GenLogID()
	ck.logInfo("%s Puting: {k:%s, v:%s}", logID, key, value)
	ck.PutAppend(logID, key, value, false)
	ck.logInfo("%s Put to <%d> succeed, {k:%s, v:%s}", logID, ck.leader, key, value)
}

func main() {
	flag.Parse()

	clerk := MakeClerk(strings.Split(ServerAddrs, ","))
	if Putting {
		if Key != "" && Value != "" {
			clerk.Put(Key, Value)
		} else {
			fmt.Println("Since you are putting something, key/value should be provided")
		}
	} else if Getting {
		if Key != "" {
			value, ok := clerk.Get(Key)
			if ok {
				fmt.Printf("%s -> %s\n", Key, value)
			} else {
				fmt.Printf("%s not exists", Key)
			}
		} else {
			fmt.Println("Since you are getting, key should be provided")
		}
	} else {
		fmt.Println("You should do something either put or get")
	}
}
