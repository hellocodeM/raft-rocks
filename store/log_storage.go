package store

import (
	"bytes"
	"encoding/binary"
	"fmt"

	. "github.com/HelloCodeMing/raft-rocks/common"

	"github.com/golang/glog"
)

// A naive log storage, based on RocksDB.
// Support append and random read
// It doesn't has any synchroization mechanism, but ensure one writer and multi readers are thread-safe.
type LogStorage struct {
	column *TableColumn

	lastIndex int // log start from 1
}

func MakeLogStorage(column *TableColumn) (*LogStorage, error) {
	glog.Infoln("Create LogStorage with column family")
	res := &LogStorage{
		column:    column,
		lastIndex: 0,
	}
	res.restoreLastIndex()
	return res, nil
}

func (l *LogStorage) restoreLastIndex() {
	iter := l.column.db.NewIteratorCF(l.column.ro, l.column.cf)
	defer iter.Close()
	iter.SeekToLast()
	if iter.Valid() {
		k := iter.Key()
		defer k.Free()
		l.lastIndex = decodeIndex(k.Data())
		glog.Infof("Restore LogStorage until index %d", l.lastIndex)
	}
}

func (l *LogStorage) Close() {
}

// encoded bytes should be sorted by numeric order, so we use binary encoding, fixed-size
func encodeIndex(index int) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(index))
	return buf
}

func decodeIndex(buf []byte) int {
	return int(binary.BigEndian.Uint32(buf))
}

func (l *LogStorage) Append(entry LogEntry) {
	buf := new(bytes.Buffer)
	entry.Encode(buf)
	l.lastIndex++
	l.column.PutBytes(encodeIndex(l.lastIndex), buf.Bytes())
}

func (l *LogStorage) AppendAt(pos int, entries []LogEntry) {
	if pos > l.lastIndex+1 {
		glog.Fatalf("No allow hole in log,lastIndex=%d,appendAt=%d", l.lastIndex, pos)
	}
	l.lastIndex = pos + len(entries) - 1
	buf := new(bytes.Buffer)
	for i, entry := range entries {
		buf.Reset()
		entry.Encode(buf)
		l.column.PutBytes(encodeIndex(pos+i), buf.Bytes())
	}
}

func (l *LogStorage) DiscardUntil(lastIndex int) {
	panic("not implemented")
}

func (l *LogStorage) At(index int) *LogEntry {
	value, ok := l.column.GetBytes(encodeIndex(index))
	if !ok {
		glog.Fatal("invalid index ", index)
	}
	entry := &LogEntry{}
	entry.Decode(bytes.NewBuffer(value))
	return entry
}

func (l *LogStorage) Slice(start, end int) []LogEntry {
	entries := make([]LogEntry, 0, end-start)
	for ; start < end; start++ {
		entries = append(entries, *l.At(start))
	}
	return entries
}

func (l *LogStorage) Last() *LogEntry {
	return l.At(l.lastIndex)
}

func (l *LogStorage) LastIndex() int {
	return l.lastIndex
}

func (l *LogStorage) String() string {
	return fmt.Sprintf("LogStorage{db:%s,lastIndex:%d}", l.column, l.LastIndex())
}
