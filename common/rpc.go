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

func (c *ClientEnd) Call(method string, args interface{}, reply interface{}) error {
	var err error
	c.client, err = rpc.DialHTTP("tcp", c.serverAddr)
	if err != nil {
		return err
	}
	return c.client.Call(method, args, reply)
}

func (c *ClientEnd) Close() {
	if c.client != nil {
		c.client.Close()
	}
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
		glog.Fatal(err)
	}
}

func MakeServerEnd(serverAddr string) *ServerEnd {
	return &ServerEnd{addr: serverAddr}
}
