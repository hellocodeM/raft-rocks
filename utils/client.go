package utils

import (
	"context"

	"github.com/HelloCodeMing/raft-rocks/pb"
	"github.com/golang/glog"
	"google.golang.org/grpc"
)

// ClientEnd implements pb.RaftClient
// TODO: delete this, use pb.RaftClient directly
type ClientEnd struct {
	server string
	client pb.RaftClient
}

func (c *ClientEnd) RequestVote(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteRes, error) {
	return c.client.RequestVote(ctx, req)
}

func (c *ClientEnd) AppendEntries(ctx context.Context, req *pb.AppendEntriesReq) (*pb.AppendEntriesRes, error) {
	return c.client.AppendEntries(ctx, req)
}

func MakeClientEnd(server string) *ClientEnd {
	conn, err := grpc.Dial(server, grpc.WithInsecure())
	if err != nil {
		glog.Error(err)
		return nil
	}
	client := pb.NewRaftClient(conn)
	return &ClientEnd{client: client}
}
