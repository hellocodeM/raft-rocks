package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/HelloCodeMing/raft-rocks/pb"
	"github.com/HelloCodeMing/raft-rocks/utils"
	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
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
	connection    *grpc.ClientConn
	client        pb.RaftKVClient

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
	var err error
	ck.connection, err = grpc.Dial(server, grpc.WithInsecure())
	if err != nil {
		glog.Warningf("connect to %s failed: %s", server, err)
		return err
	}
	ck.client = pb.NewRaftKVClient(ck.connection)
	res, err := ck.client.OpenSession(context.Background(), &pb.OpenSessionReq{})
	if err == nil && res != nil {
		ck.clientID = res.ClientId
		glog.Infof("Clerk connect to %s succeed, clientID=%d", server, ck.clientID)
		return nil
	}
	glog.Infof("Clerk connect to %s failed,error=%s", server, err)
	return err
}

func (ck *Clerk) closeSession() {
	if ck.connection != nil {
		ck.connection.Close()
	}
}

func (ck *Clerk) genGetArgs(key string) *pb.GetReq {
	ck.SN++
	args := &pb.GetReq{Key: key}
	args.Session = &pb.Session{}
	args.Session.ClientId = ck.clientID
	args.Session.LogId = utils.GenLogID()
	args.Session.Sn = ck.SN
	return args
}

// Get get a key/value
func (ck *Clerk) Get(key string) (string, bool) {
	args := ck.genGetArgs(key)
	glog.Infof("%v Getting: {k:%s}", args.Session, key)
	for r := 0; r < len(ck.serverAddrs); r++ {
		res, err := ck.client.Get(context.Background(), args)
		if err != nil {
			return "", false
		}
		if res.Status == pb.Status_OK {
			return res.Value, true
		} else if res.Status == pb.Status_NoSuchKey {
			return "", false
		}

		ck.handleError(err, res.Status)
	}
	glog.Errorf("Try many times but failed")
	return "", false
}

func (ck *Clerk) genPutArgs(key, value string) *pb.PutReq {
	ck.SN++
	args := &pb.PutReq{Key: key, Value: value}
	args.Session = &pb.Session{}
	args.Session.ClientId = ck.clientID
	args.Session.Sn = ck.SN
	args.Session.LogId = utils.GenLogID()
	return args
}

func (ck *Clerk) Put(key, value string) {
	args := ck.genPutArgs(key, value)
	glog.Infof("%s Putting: {k:%s, v:%s}", args.Session, key, value)
	for r := 0; r < len(ck.serverAddrs); r++ {
		reply, err := ck.client.Put(context.Background(), args)
		if err != nil {
			ck.handleError(err, pb.Status_OK)
		} else {
			if reply.Status == pb.Status_OK {
				return
			}
			ck.handleError(err, reply.Status)
		}
	}
}

func (ck *Clerk) handleError(rpcErr error, err pb.Status) {
	if rpcErr != nil {
		glog.Warning("rpc error,error=", rpcErr)
	} else if err != pb.Status_OK {
		glog.Warning("Internal error: ", err)
	}
	ck.rollLeader()
	time.Sleep(RetryInterval)
}

func (ck *Clerk) rollLeader() {
	ck.closeSession()
	ck.currentServer = (ck.currentServer + 1) % len(ck.serverAddrs)
	server := ck.serverAddrs[ck.currentServer]
	glog.Warningf("Try server: %s", server)
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
