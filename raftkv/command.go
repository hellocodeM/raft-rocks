package raftkv

import (
	"fmt"

	"encoding/gob"

	"golang.org/x/net/trace"
)

type KVCmdType int8

const (
	CmdGet KVCmdType = iota
	CmdPut
	CmdNoop
)

func (t KVCmdType) String() string {
	switch t {
	case CmdGet:
		return "RaftKV.Get"
	case CmdPut:
		return "RaftKV.Put"
	case CmdNoop:
		return "RaftKV.Noop"
	default:
		return "UnknownCommand"
	}
}

// command of the KV state machine
type KVCommand struct {
	CmdType  KVCmdType
	Req      interface{}
	Res      interface{}
	ClientID int64
	SN       int64
	LogID    string
	tracer   trace.Trace
}

func NewKVCommand(opType KVCmdType, req interface{}, res interface{}, clientID int64, SN int64, logID string) *KVCommand {
	cmd := &KVCommand{
		CmdType:  opType,
		Req:      req,
		Res:      res,
		ClientID: clientID,
		SN:       SN,
		LogID:    logID,
	}
	cmd.tracer = trace.New(opType.String(), fmt.Sprintf("%d:%d:%s", clientID, SN, logID))
	return cmd
}

func (op *KVCommand) trace(format string, a ...interface{}) {
	// if KVOp be replicated to follower, tracer will be nil
	if op.tracer != nil {
		msg := fmt.Sprintf(format, a...)
		op.tracer.LazyPrintf(msg)
	}
}

func init() {
	gob.Register(&KVCommand{})
}
