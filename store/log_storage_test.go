package store

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/HelloCodeMing/raft-rocks/pb"
	"github.com/stretchr/testify/assert"
)

func TestLogStorage_Basic(t *testing.T) {
	const dbName = "testrocks"
	cfName := []string{"default", "meta"}

	A := assert.New(t)
	table, columns, err := OpenTable(dbName, cfName)
	A.NoError(err)
	log, err := MakeLogStorage(columns[0])
	A.NoError(err)
	A.Equal(0, log.LastIndex())

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
	slice := log.Slice(1, 3)
	A.Equal(2, len(slice))
	A.Equal(int32(8), slice[0].Term)
	A.Equal(int32(8), slice[1].Term)

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
