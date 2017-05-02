package raft

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/trace"
)

func mockRaft() *Raft {
	return &Raft{
		persister:     MakePersister(),
		applyCh:       make(chan ApplyMsg, 1),
		me:            0,
		role:          follower,
		votedFor:      -1,
		currentTerm:   0,
		log:           NewLogManager(),
		termChangedCh: make(chan bool, 1),
		tracer:        trace.NewEventLog("MockRaft", fmt.Sprintf("peer")),
	}
}

func TestRaft_LeaderElection(t *testing.T) {
	type raftState struct {
		role        raftRole
		votedFor    int32
		currentTerm int32
	}
	type testCase struct {
		name      string
		preState  raftState
		args      RequestVoteArgs
		reply     RequestVoteReply
		postState raftState
	}
	tests := []testCase{
		testCase{
			name:      "follower meet bigger term",
			preState:  raftState{follower, -1, 1},
			args:      RequestVoteArgs{CandidateID: 1, Term: 2},
			reply:     RequestVoteReply{VoteGranted: true, Term: 2},
			postState: raftState{follower, 1, 2},
		},
		testCase{
			name:      "follower meet smaller term",
			preState:  raftState{follower, -1, 2},
			args:      RequestVoteArgs{CandidateID: 1, Term: 1},
			reply:     RequestVoteReply{VoteGranted: false, Term: 2},
			postState: raftState{follower, -1, 2},
		},
		testCase{
			name:      "candidate meet bigger term",
			preState:  raftState{candidate, 0, 1},
			args:      RequestVoteArgs{CandidateID: 1, Term: 2},
			reply:     RequestVoteReply{VoteGranted: true, Term: 2},
			postState: raftState{follower, 1, 2},
		},
		testCase{
			name:      "candidate meet small term",
			preState:  raftState{candidate, 0, 2},
			args:      RequestVoteArgs{CandidateID: 1, Term: 1},
			reply:     RequestVoteReply{VoteGranted: false, Term: 2},
			postState: raftState{candidate, 0, 2},
		},
		testCase{
			name:      "candidate meet another leader",
			preState:  raftState{candidate, 0, 2},
			args:      RequestVoteArgs{CandidateID: 1, Term: 2},
			reply:     RequestVoteReply{VoteGranted: false, Term: 2},
			postState: raftState{candidate, 0, 2},
		},
		testCase{
			name:      "leader meet bigger term",
			preState:  raftState{leader, 0, 1},
			args:      RequestVoteArgs{CandidateID: 1, Term: 2},
			reply:     RequestVoteReply{VoteGranted: true, Term: 2},
			postState: raftState{follower, 1, 2},
		},
		testCase{
			name:      "longer log",
			preState:  raftState{follower, 0, 1},
			args:      RequestVoteArgs{CandidateID: 1, Term: 2, LastLogIndex: 1024},
			reply:     RequestVoteReply{VoteGranted: true, Term: 2},
			postState: raftState{follower, 1, 2},
		},
		testCase{
			name:      "older log, but larger term",
			preState:  raftState{follower, 0, 1},
			args:      RequestVoteArgs{CandidateID: 1, Term: 2, LastLogIndex: 0, LastLogTerm: -1},
			reply:     RequestVoteReply{VoteGranted: false, Term: 2},
			postState: raftState{follower, -1, 2},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(st *testing.T) {
			A := assert.New(st)
			st.Parallel()
			raft := mockRaft()
			raft.role = test.preState.role
			raft.votedFor = test.preState.votedFor
			raft.currentTerm = test.preState.currentTerm
			reply := RequestVoteReply{}
			session := NewRequestVoteSession(0, &test.args, &reply)
			raft.processRequestVote(session)

			A.Equal(test.reply, reply)
			state := raftState{}
			state.role = raft.role
			state.votedFor = raft.votedFor
			state.currentTerm = raft.currentTerm
			A.Equal(test.postState, state)
		})
	}
}
