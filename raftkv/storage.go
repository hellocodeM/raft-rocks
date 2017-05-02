package raftkv

import (
	"sync"
)

// KVStorage storage interface
type KVStorage interface {
	Put(key, value string)
	Get(key string) (string, bool)
}

type MemKVStore struct {
	sync.RWMutex
	db map[string]string
}

func MakeMemKVStore() KVStorage {
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
