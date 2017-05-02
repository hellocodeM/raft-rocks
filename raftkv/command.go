package raftkv

import (
	"fmt"

	"golang.org/x/net/trace"
)

type KVCmdType int8

const (
	CmdGet KVCmdType = iota
	CmdPut
	CmdAppend
)

type KVCommand struct {
	CmdType  KVCmdType
	Req      interface{}
	Res      interface{}
	ClientID int
	SN       int
	LogID    string
	tracer   trace.Trace
}

func NewKVCommand(opType KVCmdType, req interface{}, res interface{}, clientID int, SN int, logID string) *KVCommand {
	return &KVCommand{
		CmdType:  opType,
		Req:      req,
		Res:      res,
		ClientID: clientID,
		SN:       SN,
		tracer:   trace.New("RaftKV.PutAppend", fmt.Sprintf("<%d:%d>", clientID, SN)),
		LogID:    logID,
	}
}

func (op *KVCommand) trace(format string, a ...interface{}) {
	// if KVOp be replicated to follower, tracer will be nil
	if op.tracer != nil {
		msg := fmt.Sprintf(format, a...)
		op.tracer.LazyPrintf(msg)
	}
}
