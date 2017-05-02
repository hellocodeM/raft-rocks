package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func makeLogManager(term int32, args ...int) LogManager {
	res := NewLogManager()
	for _, x := range args {
		res.Append(LogEntry{Term: term, Command: x})
	}
	return *res
}

func makeLogEntries(term int32, args ...int) []LogEntry {
	res := []LogEntry{}
	for _, x := range args {
		entry := LogEntry{
			Term:    term,
			Command: x,
		}
		res = append(res, entry)
	}
	return res
}

func TestRaft_Replication(t *testing.T) {
	type raftState struct {
		role        raftRole
		votedFor    int32
		currentTerm int32
	}
	type testCase struct {
		name      string
		preState  raftState
		args      AppendEntriesArgs
		reply     AppendEntriesReply
		postState raftState
	}
	tests := []testCase{
		testCase{
			name:      "follower receive heartbeat",
			preState:  raftState{follower, 1, 1},
			args:      AppendEntriesArgs{LeaderID: 1, Term: 1},
			reply:     AppendEntriesReply{Success: true, Term: 1},
			postState: raftState{follower, 1, 1},
		},
		testCase{
			name:      "follower find bigger term",
			preState:  raftState{follower, 2, 1},
			args:      AppendEntriesArgs{LeaderID: 1, Term: 2},
			reply:     AppendEntriesReply{Success: true, Term: 2},
			postState: raftState{follower, -1, 2},
		},
		testCase{
			name:      "candidate find bigger term",
			preState:  raftState{candidate, 0, 1},
			args:      AppendEntriesArgs{LeaderID: 1, Term: 2},
			reply:     AppendEntriesReply{Success: true, Term: 2},
			postState: raftState{follower, -1, 2},
		},
		testCase{
			name:      "leader find bigger term",
			preState:  raftState{leader, 0, 1},
			args:      AppendEntriesArgs{LeaderID: 1, Term: 2},
			reply:     AppendEntriesReply{Success: true, Term: 2},
			postState: raftState{follower, -1, 2},
		},
		testCase{
			name:      "reject smaller term",
			preState:  raftState{leader, 0, 2},
			args:      AppendEntriesArgs{LeaderID: 1, Term: 1},
			reply:     AppendEntriesReply{Success: false, Term: 2},
			postState: raftState{leader, 0, 2},
		},
		testCase{
			name:      "candidate find equal term",
			preState:  raftState{candidate, 0, 2},
			args:      AppendEntriesArgs{LeaderID: 1, Term: 2},
			reply:     AppendEntriesReply{Success: true, Term: 2},
			postState: raftState{candidate, 0, 2},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(st *testing.T) {
			A := assert.New(st)
			st.Parallel()
			raft := mockRaft()
			raft.role = test.preState.role
			raft.votedFor = test.preState.votedFor
			raft.currentTerm = test.preState.currentTerm
			reply := AppendEntriesReply{}
			session := NewAppendEntriesSession(0, raft.currentTerm, &test.args, &reply)
			raft.processAppendEntries(session)

			A.Equal(test.reply, reply)
			state := raftState{}
			state.role = raft.role
			state.votedFor = raft.votedFor
			state.currentTerm = raft.currentTerm
			A.Equal(test.postState, state)
		})
	}

}

func TestRaft_AppendEntries(t *testing.T) {
	type raftState struct {
		role        raftRole
		votedFor    int32
		currentTerm int32
		log         LogManager
	}
	type testCase struct {
		name      string
		preState  raftState
		args      AppendEntriesArgs
		reply     AppendEntriesReply
		postState raftState
	}
	tests := []testCase{
		testCase{
			name:      "simple entries",
			preState:  raftState{follower, 1, 1, *NewLogManager()},
			args:      AppendEntriesArgs{LeaderID: 1, Term: 1, PrevLogIndex: 0, PrevLogTerm: 0, Entries: makeLogEntries(1, 1)},
			reply:     AppendEntriesReply{Term: 1, Success: true},
			postState: raftState{follower, 1, 1, makeLogManager(1, 1)},
		},
		testCase{
			name:      "not existed prevLog",
			preState:  raftState{follower, 1, 1, makeLogManager(1, 1)},
			args:      AppendEntriesArgs{LeaderID: 1, Term: 1, PrevLogIndex: 5, PrevLogTerm: 0, Entries: makeLogEntries(1, 1)},
			reply:     AppendEntriesReply{Term: 1, Success: false},
			postState: raftState{follower, 1, 1, makeLogManager(1, 1)},
		},
		testCase{
			name:      "mismatch term",
			preState:  raftState{follower, 1, 1, makeLogManager(1, 1)},
			args:      AppendEntriesArgs{LeaderID: 1, Term: 1, PrevLogIndex: 1, PrevLogTerm: 0, Entries: makeLogEntries(1, 2)},
			reply:     AppendEntriesReply{Term: 1, Success: false},
			postState: raftState{follower, 1, 1, makeLogManager(1, 1)},
		},
		testCase{
			name:      "overwrite existed logs",
			preState:  raftState{follower, 1, 1, makeLogManager(1, 1, 2, 3)},
			args:      AppendEntriesArgs{LeaderID: 1, Term: 1, PrevLogIndex: 0, PrevLogTerm: 0, Entries: makeLogEntries(1, 2, 3, 4)},
			reply:     AppendEntriesReply{Term: 1, Success: true},
			postState: raftState{follower, 1, 1, makeLogManager(1, 2, 3, 4)},
		},
		testCase{
			name:      "overwrite partitial log",
			preState:  raftState{follower, 1, 1, makeLogManager(1, 1, 2, 3, 4)},
			args:      AppendEntriesArgs{LeaderID: 1, Term: 1, PrevLogIndex: 0, PrevLogTerm: 0, Entries: makeLogEntries(1, 5, 6)},
			reply:     AppendEntriesReply{Term: 1, Success: true},
			postState: raftState{follower, 1, 1, makeLogManager(1, 5, 6, 3, 4)},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(st *testing.T) {
			A := assert.New(st)
			st.Parallel()
			raft := mockRaft()
			raft.role = test.preState.role
			raft.votedFor = test.preState.votedFor
			raft.currentTerm = test.preState.currentTerm
			raft.log = &test.preState.log
			reply := AppendEntriesReply{}
			session := NewAppendEntriesSession(0, raft.currentTerm, &test.args, &reply)
			raft.processAppendEntries(session)

			A.Equal(test.reply, reply)
			state := raftState{}
			state.role = raft.role
			state.votedFor = raft.votedFor
			state.currentTerm = raft.currentTerm
			state.log = *raft.log
			A.Equal(test.postState, state)
		})
	}
}

func makeMixedLog(entries ...LogEntry) LogManager {
	res := NewLogManager()
	for _, e := range entries {
		res.Append(e)
	}
	return *res
}

func TestRaft_Retreat(t *testing.T) {
	type raftState struct {
		nextIndex         int
		lastIncludedIndex int
		log               LogManager
	}
	type testCase struct {
		name      string
		preState  raftState
		postState raftState
	}
	targetPeer := 1
	tests := []testCase{
		testCase{
			name:      "empty log",
			preState:  raftState{1, 0, makeLogManager(1)},
			postState: raftState{1, 0, makeLogManager(1)},
		},
		testCase{
			name:      "single term, to init point",
			preState:  raftState{3, 0, makeLogManager(1, -1, -2)},
			postState: raftState{1, 0, makeLogManager(1, -1, -2)},
		},
		testCase{
			name:      "mixed term, to previous term",
			preState:  raftState{3, 0, makeMixedLog(LogEntry{1, -1}, LogEntry{2, -2}, LogEntry{3, -3})},
			postState: raftState{2, 0, makeMixedLog(LogEntry{1, -1}, LogEntry{2, -2}, LogEntry{3, -3})},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(st *testing.T) {
			A := assert.New(st)
			st.Parallel()
			raft := mockRaft()
			raft.nextIndex = make([]int, targetPeer+1)
			raft.nextIndex[targetPeer] = test.preState.nextIndex
			raft.log = &test.preState.log
			raft.lastIncludedIndex = test.preState.lastIncludedIndex

			raft.retreatForPeer(targetPeer)

			state := raftState{
				nextIndex:         raft.nextIndex[targetPeer],
				log:               *raft.log,
				lastIncludedIndex: raft.lastApplied,
			}
			A.Equal(test.postState, state)
		})
	}
}
