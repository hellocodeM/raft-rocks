package store

import (
	"sync"

	"github.com/golang/glog"
)

// KVStorage storage interface
type KVStorage interface {
	Put(key, value string)
	Get(key string) (string, bool)
	Close()
}

type MemKVStore struct {
	sync.RWMutex
	db map[string]string
}

func MakeMemKVStore() *MemKVStore {
	return &MemKVStore{
		db: make(map[string]string),
	}
}

func (s *MemKVStore) Put(key, value string) {
	s.Lock()
	defer s.Unlock()
	s.db[key] = value
}

func (s *MemKVStore) Get(key string) (string, bool) {
	s.RLock()
	defer s.RUnlock()
	res, ok := s.db[key]
	return res, ok
}

func (s *MemKVStore) Close() {
}

type RocksDBStore struct {
	column *TableColumn
}

func MakeRocksDBStore(column *TableColumn) (*RocksDBStore, error) {
	res := &RocksDBStore{column: column}
	return res, nil
}

func (s *RocksDBStore) Close() {
	glog.Info("Close RocksDB KVStore")
}

func (s *RocksDBStore) Get(key string) (string, bool) {
	return s.column.Get(key)
}

func (s *RocksDBStore) Put(key, value string) {
	s.column.Put(key, value)
}
