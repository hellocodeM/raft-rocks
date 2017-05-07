package store

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/HelloCodeMing/raft-rocks/pb"
	"github.com/stretchr/testify/assert"
)

var (
	dbName = "testrocks"
	cfName = []string{"default", "meta"}
)

func TestLogStorage_Basic(t *testing.T) {
	A := assert.New(t)
	table, columns, err := OpenTable(dbName, cfName)
	A.NoError(err)
	log, err := MakeLogStorage(columns[0])
	A.NoError(err)

	// [nil]
	A.Equal(0, log.LastIndex())
	A.Equal(int32(0), log.Last().Term)

	// append an entry
	entry := &pb.KVCommand{Term: 7}
	log.Append(entry)
	A.Equal(1, log.LastIndex())

	A.Equal(entry, log.At(1))

	// append multi entries
	// [nil, 7]
	// [nil, 7, 7]
	// [nil, 8, 8, 8]
	log.Append(entry)
	entry.Term = 8
	entries := []*pb.KVCommand{entry, entry, entry}
	log.AppendAt(1, entries)

	A.Equal(int32(8), log.At(1).Term)
	A.Equal(int32(8), log.At(2).Term)
	A.Equal(int32(8), log.At(3).Term)
	A.Equal(3, log.LastIndex())
	A.Equal(int32(8), log.Last().Term)

	// Slice
	slice := log.Slice(1, 5)
	A.Equal(3, len(slice))
	A.Equal(int32(8), slice[0].Term)
	A.Equal(int32(8), slice[1].Term)
	A.Equal(int32(8), slice[2].Term)

	// String
	fmt.Println(log)

	// close and restore
	log.Close()
	table.Close()
	table, columns, err = OpenTable(dbName, cfName)
	A.NoError(err)
	log, err = MakeLogStorage(columns[0])
	A.NoError(err)

	A.Equal(3, log.LastIndex())

	log.Close()
	table.Close()
	os.RemoveAll(dbName)
}

func TestEncodeIndex(t *testing.T) {
	n1 := rand.Intn(1000)
	n2 := rand.Intn(1000)
	b1 := encodeIndex(n1)
	b2 := encodeIndex(n2)
	A := assert.New(t)
	A.Equal(n1 < n2, bytes.Compare(b1, b2) == -1)
	A.Equal(n1, decodeIndex(b1))
	A.Equal(n2, decodeIndex(b2))
}

func TestConcurrentRead(t *testing.T) {
	os.RemoveAll(dbName)
	A := assert.New(t)
	table, columns, err := OpenTable(dbName, cfName)
	A.NoError(err)
	defer os.RemoveAll(dbName)
	defer table.Close()
	log, err := MakeLogStorage(columns[0])
	A.NoError(err)

	// create one goroutine continously append log,
	// and another goroutine perform random read
	// the reading result should be consistent
	const N = 10000
	const Concurrency = 10
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i < N; i++ {
			entry := &pb.KVCommand{Index: int32(i)}
			log.Append(entry)
		}
	}()
	wg.Add(Concurrency)
	for k := 0; k < Concurrency; k++ {
		go func() {
			defer wg.Done()
			for i := 1; i < N; i++ {
				index := rand.Intn(i)
				if log.LastIndex() < index {
					index = log.LastIndex()
				}
				entry := log.At(index)
				A.NotNil(entry)
				A.Equal(int32(index), entry.Index)
			}
		}()
	}
	wg.Wait()
}
