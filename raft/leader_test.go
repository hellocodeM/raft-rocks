package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRaft_LeaderCommit(t *testing.T) {
	type raftState struct {
		currentTerm int32
		commitIndex int
		matchIndex  []int
		log         LogManager
	}
	type testCase struct {
		name      string
		preState  raftState
		postState raftState
	}
	mixedLog := makeLogManager(6, -1, -2, -3, -4, -5)
	mixedLog.Append(LogEntry{7, -6})
	tests := []testCase{
		testCase{
			name:      "nothing to commit",
			preState:  raftState{1, 0, []int{0, 0, 0}, makeLogManager(1)},
			postState: raftState{1, 0, []int{0, 0, 0}, makeLogManager(1)},
		},
		testCase{
			name:      "majority commit",
			preState:  raftState{7, 2, []int{0, 2, 0}, makeLogManager(7, -1, -2)},
			postState: raftState{7, 2, []int{0, 2, 0}, makeLogManager(7, -1, -2)},
		},
		testCase{
			name:      "Figure8 not commit previous term",
			preState:  raftState{7, 2, []int{0, 5, 0}, makeLogManager(6, -1, -2, -3, -4, -5)},
			postState: raftState{7, 2, []int{0, 5, 0}, makeLogManager(6, -1, -2, -3, -4, -5)},
		},
		testCase{
			name:      "Figure8 commit this term",
			preState:  raftState{7, 2, []int{0, 6, 0}, mixedLog},
			postState: raftState{7, 6, []int{0, 6, 0}, mixedLog},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(st *testing.T) {
			A := assert.New(st)
			st.Parallel()
			raft := mockRaft()
			raft.currentTerm = test.preState.currentTerm
			raft.commitIndex = test.preState.commitIndex
			raft.matchIndex = test.preState.matchIndex
			raft.log = &test.preState.log

			raft.leaderCommit()

			state := raftState{}
			state.currentTerm = raft.currentTerm
			state.commitIndex = raft.commitIndex
			state.matchIndex = raft.matchIndex
			state.log = *raft.log
			A.Equal(test.postState, state)
		})
	}

}
