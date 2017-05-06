package utils

import (
	"context"

	"github.com/HelloCodeMing/raft-rocks/pb"
	"google.golang.org/grpc"
)

// ClientEnd implements pb.RaftClient
type ClientEnd struct {
	server string
	client pb.RaftClient
}

func (c *ClientEnd) RequestVote(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteRes, error) {
	if c.client == nil {
		err := c.connect()
		if err != nil {
			return nil, err
		}
	}
	return c.client.RequestVote(ctx, req)
}

func (c *ClientEnd) AppendEntries(ctx context.Context, req *pb.AppendEntriesReq) (*pb.AppendEntriesRes, error) {
	if c.client == nil {
		err := c.connect()
		if err != nil {
			return nil, err
		}
	}
	return c.client.AppendEntries(ctx, req)
}

func (c *ClientEnd) connect() error {
	conn, err := grpc.Dial(c.server)
	if err == nil {
		c.client = pb.NewRaftClient(conn)
		return nil
	}
	return err
}

func MakeClientEnd(server string) *ClientEnd {
	return &ClientEnd{server: server}
}
