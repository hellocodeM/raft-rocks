package main

import (
	"flag"
	_ "net/http/pprof"
	"strings"

	"github.com/HelloCodeMing/raft-rocks/common"
	"github.com/HelloCodeMing/raft-rocks/raft"
	"github.com/HelloCodeMing/raft-rocks/raftkv"
	"github.com/golang/glog"
)

var (
	ServerAddr        string
	Replicas          string
	SnapshotThreshold int
)

func init() {
	flag.StringVar(&ServerAddr, "address", "localhost:10000", "address server listen to")
	flag.StringVar(&Replicas, "replicas", "localhost:10000,localhost:10001,localhost:10002", "all replicas in raft group")
	flag.IntVar(&SnapshotThreshold, "snapshot_threshold", 1024*1024, "when this threshold reached, do snapshot")
}

func main() {
	flag.Parse()
	glog.Infof("Start raftkv at %s", ServerAddr)
	defer glog.Infof("Stop raft-rocks")

	// create clientEnds
	servers := make([]*common.ClientEnd, 0, len(Replicas)-1)
	me := -1
	for i, replica := range strings.Split(Replicas, ",") {
		servers = append(servers, common.MakeClientEnd(replica))
		if replica == ServerAddr {
			me = i
		}
	}
	if me == -1 {
		glog.Fatalf("replicas not contains the server itself,this=%s,replicas=%s", ServerAddr, Replicas)
	}
	// start server
	raftkv := raftkv.StartKVServer(servers, me, raft.MakePersister(), SnapshotThreshold)
	server := common.MakeServerEnd(ServerAddr)
	server.AddService(raftkv)
	server.Serve()
}
