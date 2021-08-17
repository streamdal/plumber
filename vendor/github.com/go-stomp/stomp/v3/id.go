package stomp

import (
	"strconv"
	"sync/atomic"
)

var _lastId uint64

// allocateId returns a unique number for the current
// process. Starts at one and increases. Used for
// allocating subscription ids, receipt ids,
// transaction ids, etc.
func allocateId() string {
	id := atomic.AddUint64(&_lastId, 1)
	return strconv.FormatUint(id, 10)
}
