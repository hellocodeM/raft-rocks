package raft

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogManager_NewLogManager(t *testing.T) {
	lm := NewLogManager()
	assert.Equal(t, int32(0), lm.At(0).Term)
}

func TestLogManager_Append(t *testing.T) {
	lm := NewLogManager()
	lm.Append(LogEntry{Term: 1})
	assert.Equal(t, int32(1), lm.Last().Term)
}

func TestLogManager_AppendAt(t *testing.T) {
	lm := NewLogManager()
	entries := []LogEntry{
		LogEntry{Term: 1},
		LogEntry{Term: 2},
	}
	lm.AppendAt(1, entries)
	assert.Equal(t, int32(1), lm.At(1).Term)
	assert.Equal(t, int32(2), lm.At(2).Term)

	lm.AppendAt(2, []LogEntry{LogEntry{Term: 3}, LogEntry{Term: 4}})
	assert.Equal(t, int32(3), lm.At(2).Term, "overwrite")
	assert.Equal(t, int32(4), lm.At(3).Term, "overwrite")
}

func TestLogManager_DiscardUntil(t *testing.T) {
	A := assert.New(t)
	lm := NewLogManager()
	lm.Append(LogEntry{1, 1024})
	lm.Append(LogEntry{2, 1024})

	lm.DiscardUntil(1)
	A.Equal(3, lm.Length())
	A.Panics(func() { lm.At(1) }, "discarded log")
	A.Equal(1024, lm.At(2).Command, "shorter than length")

	lm.DiscardUntil(3)
	A.Equal(4, lm.Length())
	A.Panics(func() { lm.At(3) }, "discarded log")
}

func TestLogManager_EncodeDecode(t *testing.T) {
	t.Run("nonempty log", func(t *testing.T) {
		lm := NewLogManager()
		for i := 0; i < 100; i++ {
			lm.Append(LogEntry{int32(i), i})
		}

		buff := new(bytes.Buffer)
		lm.Encode(buff)
		t.Logf("100 entry bytes: %d", buff.Len())

		lmCopy := NewLogManager()
		lmCopy.Decode(buff)
		assert.Equal(t, lm.log, lmCopy.log)
	})
	t.Run("empty log", func(t *testing.T) {
		lm := NewLogManager()
		buff := new(bytes.Buffer)
		lm.Encode(buff)
		t.Logf("0 entry bytes: %d", buff.Len())
		lmCopy := NewLogManager()
		lmCopy.Decode(buff)
		assert.Equal(t, lm.log, lmCopy.log)
	})
}
