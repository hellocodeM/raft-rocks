package raft

import (
	"encoding/gob"
	"fmt"
	"io"
)

type LogEntry struct {
	Term    int32
	Command interface{}
}

type LogManager struct {
	log    []LogEntry
	offset int
}

func NewLogManager() *LogManager {
	return &LogManager{
		log: make([]LogEntry, 1),
	}
}

func (lm *LogManager) Append(entry LogEntry) {
	lm.log = append(lm.log, entry)
}

func (lm *LogManager) AppendAt(pos int, entries []LogEntry) {
	last := pos + len(entries)
	if lm.LastIndex() < last {
		lm.log = append(lm.log[:pos], entries...)
	} else {
		copy(lm.log[pos:], entries)
	}
}

func (lm *LogManager) DiscardUntil(lastIndex int) {
	lm.offset = lastIndex + 1
	if lastIndex < lm.LastIndex() {
		remain := lm.log[lastIndex+1:]
		lm.log = make([]LogEntry, lastIndex+1)
		lm.log = append(lm.log, remain...)
	} else {
		lm.log = make([]LogEntry, lastIndex+1)
	}
}

func (lm *LogManager) At(index int) *LogEntry {
	if index < lm.offset {
		panic(fmt.Sprintf("log has been discard %d < %d", index, lm.offset))
	}
	if index > lm.LastIndex() {
		panic(fmt.Sprintf("out of log index: %d>%d, %s", index, lm.LastIndex(), lm.String()))
	}
	return &lm.log[index]
}

func (lm *LogManager) Slice(start int, end int) []LogEntry {
	return lm.log[start:end]
}

func (lm *LogManager) Last() *LogEntry {
	return lm.At(lm.LastIndex())
}

func (lm *LogManager) LastIndex() int {
	return lm.Length() - 1
}

func (lm *LogManager) Length() int {
	return len(lm.log)
}

func (lm *LogManager) String() string {
	const Max = 5
	start := maxInt(lm.LastIndex()-Max, lm.offset)
	return fmt.Sprintf("log[%d:]=%v", start, lm.log[start:])
}

func (lm *LogManager) Encode(writer io.Writer) error {
	encoder := gob.NewEncoder(writer)
	encoder.Encode(lm.offset)
	encoder.Encode(lm.log[lm.offset:])
	return nil
}

func (lm *LogManager) Decode(reader io.Reader) error {
	decoder := gob.NewDecoder(reader)
	decoder.Decode(&lm.offset)
	lm.log = make([]LogEntry, lm.offset)
	tmp := []LogEntry{}
	decoder.Decode(&tmp)
	lm.log = append(lm.log, tmp...)
	return nil
}
