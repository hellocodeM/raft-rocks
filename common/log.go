package common

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/golang/glog"
)

const (
	VDebug glog.Level = iota
	VDump
)

// GenLogID generate a unique log id for tracing
func GenLogID() string {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	return fmt.Sprintf("LogID<%4d>", random.Int()%1E4)
}
