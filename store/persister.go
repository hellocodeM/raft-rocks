package store

import (
	"encoding/binary"
)

// Persister store persistent state in raft
type Persister interface {
	LoadInt32(key string) (int32, bool)
	StoreInt32(key string, value int32)
	LoadInt64(key string) (int64, bool)
	StoreInt64(key string, value int64)
}

// RocksBasedPersister a naive implementation, store key/value directly
type RocksBasedPersister struct {
	column *TableColumn
}

var persisterKey = []byte("persister")

func MakeRocksBasedPersister(column *TableColumn) *RocksBasedPersister {
	return &RocksBasedPersister{column: column}
}

func (p *RocksBasedPersister) LoadInt32(key string) (int32, bool) {
	value, ok := p.column.GetBytes([]byte(key))
	if ok {
		return int32(binary.BigEndian.Uint32(value)), ok
	}
	return 0, ok
}

func (p *RocksBasedPersister) StoreInt32(key string, value int32) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(value))
	p.column.PutBytes([]byte(key), buf)
}

func (p *RocksBasedPersister) LoadInt64(key string) (int64, bool) {
	value, ok := p.column.GetBytes([]byte(key))
	if ok {
		return int64(binary.BigEndian.Uint64(value)), ok
	}
	return 0, ok
}

func (p *RocksBasedPersister) StoreInt64(key string, value int64) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(value))
	p.column.PutBytes([]byte(key), buf)
}
