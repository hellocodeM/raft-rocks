package common

import (
	"fmt"
	"math/rand"
)

type PutArgs struct {
	Key   string
	Value string

	SN       int // unique serial number for each client
	ClientID int // clientId for tracing
	LogID    string
}

type PutReply struct {
	Err ClerkResult
}

type GetArgs struct { // implements ClerkArgs
	Key string

	SN       int
	ClientID int
	LogID    string
}

type GetReply struct {
	Err   ClerkResult
	Value string
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

func GenLogID() string {
	return fmt.Sprintf("<<<%d>>>", rand.Int())
}
