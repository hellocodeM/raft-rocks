package common

import (
	"encoding/gob"
	"io"
)

type LogEntry struct {
	Term    int32
	Command *KVCommand
}

func (e *LogEntry) Encode(writer io.Writer) {
	encoder := gob.NewEncoder(writer)
	encoder.Encode(e)
}

func (e *LogEntry) Decode(reader io.Reader) {
	decoder := gob.NewDecoder(reader)
	decoder.Decode(e)
}
