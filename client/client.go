package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/HelloCodeMing/raft-rocks/common"
	"github.com/golang/glog"
)

var (
	ServerAddrs   string
	RetryInterval time.Duration

	// client actions
	Putting bool
	Getting bool
	Key     string
	Value   string

	// errors
	ErrUnavailableServer = errors.New("server unavailable")
)

func init() {
	flag.StringVar(&ServerAddrs, "server_addrs", "localhost:10000,localhost:10001,localhost:10002", "server addresses")
	flag.DurationVar(&RetryInterval, "retry_interval", time.Second, "interval retry a failed server")
	flag.BoolVar(&Putting, "put", false, "put a key/value")
	flag.BoolVar(&Getting, "get", false, "get a key/value")
	flag.StringVar(&Key, "key", "", "key")
	flag.StringVar(&Value, "value", "", "value")
}

// Clerk client implementation
type Clerk struct {
	serverAddrs   []string
	currentServer int
	client        *common.ClientEnd

	clientID int64
	SN       int64
}

// MakeClerk create a client
func MakeClerk(servers []string) (*Clerk, error) {
	ck := new(Clerk)
	ck.serverAddrs = servers
	if err := ck.openSession(); err != nil {
		return nil, err
	}
	glog.Infof("Created clerk, clientID:%d", ck.clientID)
	return ck, nil
}

func (ck *Clerk) openSession() error {
	servers := ck.serverAddrs
	for _, addr := range servers {
		if ck.connect(addr) == nil {
			return nil
		}
	}
	return ErrUnavailableServer
}

func (ck *Clerk) connect(server string) error {
	ck.client = common.MakeClientEnd(server)
	reply := &common.OpenSessionReply{}
	err := ck.client.Call("RaftKV.OpenSession", &common.OpenSessionArgs{}, reply)
	if err == nil && reply.Status == common.OK {
		ck.clientID = reply.ClientID
		glog.Infof("Clerk connect to %s succeed", server)
		return nil
	}
	glog.Infof("Clerk connect to %s failed,error=%s", server, err)
	return err
}

func (ck *Clerk) closeSession() {
	if ck.client != nil {
		ck.client.Close()
	}
}

func (ck *Clerk) genGetArgs(key string) *common.GetArgs {
	ck.SN++
	args := &common.GetArgs{Key: key}
	args.ClientID = ck.clientID
	args.LogID = common.GenLogID()
	args.SN = ck.SN
	return args
}

// Get get a key/value
func (ck *Clerk) Get(key string) (string, bool) {
	args := ck.genGetArgs(key)
	glog.Infof("%s Getting: {k:%s}", args.LogID, key)
	for r := 0; r < len(ck.serverAddrs); r++ {
		reply := &common.GetReply{}
		err := ck.client.Call("RaftKV.Get", &args, reply)
		if err == nil && reply.Status == common.OK {
			return reply.Value, true
		} else if reply.Status == common.ErrNoKey {
			return "", false
		}

		ck.handleError(err, reply.Status)
	}
	glog.Errorf("Try many times but failed")
	return "", false
}

func (ck *Clerk) genPutArgs(key, value string) *common.PutArgs {
	ck.SN++
	args := &common.PutArgs{
		Key:   key,
		Value: value,
	}
	args.ClientID = ck.clientID
	args.SN = ck.SN
	args.LogID = common.GenLogID()
	return args
}

func (ck *Clerk) Put(key, value string) {
	args := ck.genPutArgs(key, value)
	glog.Infof("%s Putting: {k:%s, v:%s}", args.LogID, key, value)
	for r := 0; r < len(ck.serverAddrs); r++ {
		reply := &common.PutReply{}
		err := ck.client.Call("RaftKV.Put", &args, reply)
		if err == nil && reply.Status == common.OK {
			return
		}
		ck.handleError(err, reply.Status)
	}
}

func (ck *Clerk) handleError(rpcErr error, err common.ClerkResult) {
	if rpcErr != nil {
		glog.Warning("rpc error,error=", rpcErr)
	} else if err != common.OK {
		glog.Warning(err)
	}
	ck.rollLeader()
	glog.Warningf("try server: %d", ck.currentServer)
	time.Sleep(RetryInterval)
}

func (ck *Clerk) rollLeader() {
	ck.closeSession()
	ck.currentServer = (ck.currentServer + 1) % len(ck.serverAddrs)
	server := ck.serverAddrs[ck.currentServer]
	ck.connect(server)
}

func main() {
	flag.Parse()

	clerk, err := MakeClerk(strings.Split(ServerAddrs, ","))
	if err != nil {
		fmt.Println("Could not connect to any server")
		os.Exit(1)
	}
	defer clerk.closeSession()
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
