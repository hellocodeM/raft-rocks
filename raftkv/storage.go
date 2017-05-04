package raftkv

import (
	"sync"

	"github.com/golang/glog"
	"github.com/tecbot/gorocksdb"
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
	db           *gorocksdb.DB
	readOptions  *gorocksdb.ReadOptions
	writeOptions *gorocksdb.WriteOptions
}

func MakeRocksDBStore(path string) (*RocksDBStore, error) {
	options := gorocksdb.NewDefaultOptions()
	options.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(options, path)
	if err != nil {
		glog.Warningf("Failed to open rocksdb at %s,error=%s", path, err)
		return nil, err
	}

	res := &RocksDBStore{db: db}
	res.readOptions = gorocksdb.NewDefaultReadOptions()
	res.writeOptions = gorocksdb.NewDefaultWriteOptions()
	return res, nil
}

func (s *RocksDBStore) Close() {
	s.db.Close()
	glog.Info("Close rocksdb")
}

func (s *RocksDBStore) Get(key string) (string, bool) {
	value, err := s.db.Get(s.readOptions, []byte(key))
	defer value.Free()
	if err != nil {
		glog.Warningf("Get from rocksdb failed due to %s", err)
		return "", false
	} else if value.Data() == nil {
		return "", false
	}

	return string(value.Data()), true
}

func (s *RocksDBStore) Put(key, value string) {
	err := s.db.Put(s.writeOptions, []byte(key), []byte(value))
	if err != nil {
		glog.Warningf("Put to rocksdb failed due to %s", err)
	}
}
