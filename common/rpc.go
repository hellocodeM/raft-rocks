package common

import (
	"net/http"
	"net/rpc"

	"github.com/golang/glog"
)

type ClientEnd struct {
	client     *rpc.Client
	serverAddr string
}

func MakeClientEnd(serverAddr string) *ClientEnd {
	return &ClientEnd{serverAddr: serverAddr}
}

func (c *ClientEnd) Call(method string, args interface{}, reply interface{}) bool {
	if c.client == nil {
		client, err := rpc.DialHTTP("tcp", c.serverAddr)
		if err != nil {
			glog.Warningf("Failed to dial server %s", c.serverAddr)
			return false
		}
		c.client = client
		glog.Warningf("Connection could not be established, call %s failed", method)
	}
	return c.client.Call(method, args, reply) == nil
}

func (c *ClientEnd) connect() {
}

type ServerEnd struct {
	addr string
}

func (s *ServerEnd) AddService(service interface{}) {
	if err := rpc.Register(service); err != nil {
		panic(err)
	}
}

func (s *ServerEnd) Serve() {
	rpc.HandleHTTP()
	err := http.ListenAndServe(s.addr, nil)
	if err != nil {
		panic(err)
	}
}

func MakeServerEnd(serverAddr string) *ServerEnd {
	return &ServerEnd{addr: serverAddr}
}
