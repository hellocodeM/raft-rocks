package common

import (
	"encoding/gob"
)

// To implement linearizable semantic
type ArgsBase struct {
	ClientID int64  // unique for each client
	SN       int64  // unique serial number for each client
	LogID    string // for tracing
}

// Create Session
type OpenSessionArgs struct {
}

type OpenSessionReply struct {
	Status   ClerkResult
	ClientID int64
}

// Put(key, value)
type PutArgs struct {
	ArgsBase
	Key   string
	Value string
}

type PutReply struct {
	Status ClerkResult
}

// Value = Get(key)
type GetArgs struct { // implements ClerkArgs
	ArgsBase
	Key string
}

type GetReply struct {
	Status ClerkResult
	Value  string
}

type ClerkResult int8

const (
	OK ClerkResult = iota
	ErrTimeout
	ErrNoKey
	ErrNotLeader
)

func (error ClerkResult) String() string {
	switch error {
	case OK:
		return "OK"
	case ErrNoKey:
		return "ErrNoKey"
	case ErrNotLeader:
		return "ErrNotLeader"
	case ErrTimeout:
		return "ErrTimeout"
	default:
		return "Unknown"
	}
}

func init() {
	gob.Register(&PutArgs{})
	gob.Register(&PutReply{})
	gob.Register(&GetArgs{})
	gob.Register(&GetReply{})
}
