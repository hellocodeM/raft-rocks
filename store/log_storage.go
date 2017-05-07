package store

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"io"

	"github.com/HelloCodeMing/raft-rocks/pb"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
)

// LogStorage  A naive log storage, based on RocksDB.
// Support append and random read
type LogStorage struct {
	column *TableColumn

	mu        sync.Mutex
	lastIndex int32 // log start from 1, at 0 it's an empty entry.
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
		l.lastIndex = int32(decodeIndex(k.Data()))
		glog.Infof("Restore LogStorage until index %d", l.lastIndex)
	} else {
		glog.Infof("This is a fresh log, create a placeholder at 0")
		ph := &pb.KVCommand{}
		l.AppendAt(0, []*pb.KVCommand{ph})
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

func (l *LogStorage) Append(entry *pb.KVCommand) int {
	// Append consists of two operation, update lastIndex, and append it to log
	// althrouth we could make updating atomic, but append could not
	// if we update lastIndex first, then append. The result will be that other goroutine could not read at lastIndex
	// if we append first, then update lastIndex. The result will be that the log will overlap, because here append is not a real append,
	// it's just a put operation
	// so we protect this procedure with a mutex
	l.mu.Lock()
	defer l.mu.Unlock()
	newIndex := int(atomic.LoadInt32(&l.lastIndex)) + 1
	entry.Index = int32(newIndex)
	bs, err := proto.Marshal(entry)
	if err != nil {
		glog.Error("append fail: %s", err)
	}
	l.column.PutBytes(encodeIndex(newIndex), bs)
	atomic.AddInt32(&l.lastIndex, 1)
	return newIndex
}

func (l *LogStorage) AppendAt(pos int, entries []*pb.KVCommand) {
	if pos > l.LastIndex()+1 {
		glog.Fatalf("No allow hole in log,lastIndex=%d,appendAt=%d", l.lastIndex, pos)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, entry := range entries {
		entry.Index = int32(pos + i)
		bs, err := proto.Marshal(entry)
		if err != nil {
			glog.Errorf("append fail: %s", err)
		}
		l.column.PutBytes(encodeIndex(pos+i), bs)
	}
	atomic.StoreInt32(&l.lastIndex, int32(pos+len(entries)-1))
}

func (l *LogStorage) DiscardUntil(lastIndex int) {
	panic("not implemented")
}

func (l *LogStorage) At(index int) *pb.KVCommand {
	value, ok := l.column.GetBytes(encodeIndex(index))
	if !ok {
		panic("invalid index")
	}
	entry := &pb.KVCommand{}
	proto.Unmarshal(value, entry)
	return entry
}

func (l *LogStorage) Slice(start, end int) []*pb.KVCommand {
	if end > l.LastIndex()+1 {
		end = l.LastIndex() + 1
	}
	entries := make([]*pb.KVCommand, 0, end-start)
	for ; start < end; start++ {
		entries = append(entries, l.At(start))
	}
	return entries
}

func (l *LogStorage) Last() *pb.KVCommand {
	return l.At(int(l.LastIndex()))
}

func (l *LogStorage) LastIndex() int {
	return int(atomic.LoadInt32(&l.lastIndex))
}

func (l *LogStorage) String() string {
	return fmt.Sprintf("LogStorage{db:%s,lastIndex:%d}", l.column, l.LastIndex())
}

func (l *LogStorage) Dump(writer io.Writer) {
	db := l.column.db
	iter := db.NewIteratorCF(l.column.ro, l.column.cf)
	defer iter.Close()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		k := decodeIndex(iter.Key().Data())
		v := &pb.KVCommand{}
		proto.Unmarshal(iter.Value().Data(), v)
		writer.Write([]byte(fmt.Sprintf("%d : %s\n", k, v)))
	}
	writer.Write([]byte("\n"))
}
