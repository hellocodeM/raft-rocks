package main

import (
	"flag"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/HelloCodeMing/raft-rocks/pb"
	"github.com/HelloCodeMing/raft-rocks/raftkv"
	"github.com/HelloCodeMing/raft-rocks/utils"
	"github.com/golang/glog"
	"google.golang.org/grpc"
)

var (
	RpcAddr   string
	Replicas  string
	DebugAddr string
)

func init() {
	flag.StringVar(&DebugAddr, "debug_address", "localhost:8000", "use for debug, pprof")
	flag.StringVar(&RpcAddr, "rpc_address", "localhost:10000", "rpc server's address")
	flag.StringVar(&Replicas, "replicas", "localhost:10000,localhost:10001,localhost:10002", "all replicas in raft group")

	// before shutdown, flush log
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		glog.Flush()
		os.Exit(1)
	}()
}

// /debug/requests, /debug/pprof, /raft
func runDebugServer() {
	http.ListenAndServe(DebugAddr, nil)
}

func main() {
	flag.Parse()
	glog.Infof("Start raftkv at %s", RpcAddr)
	go runDebugServer()

	// create clientEnds
	servers := make([]*utils.ClientEnd, 0, len(Replicas))
	me := -1
	for i, replica := range strings.Split(Replicas, ",") {
		servers = append(servers, utils.MakeClientEnd(replica))
		if replica == RpcAddr {
			me = i
		}
	}
	if me == -1 {
		glog.Fatalf("replicas does not contains the server itself,this=%s,replicas=%s", RpcAddr, Replicas)
	}

	// start server
	server := grpc.NewServer()
	raftkv := raftkv.StartRaftKV(server, servers, me)
	lis, err := net.Listen("tcp", RpcAddr)
	if err != nil {
		glog.Fatal(err)
	}
	pb.RegisterRaftKVServer(server, raftkv)
	server.Serve(lis)
}
