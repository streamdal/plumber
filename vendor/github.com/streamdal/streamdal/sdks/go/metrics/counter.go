package metrics

import (
	"strings"
	"sync"
	"time"

	"github.com/streamdal/streamdal/sdks/go/types"
)

type counter struct {
	entry       *types.CounterEntry
	count       int64
	countMutex  *sync.RWMutex
	lastUpdated time.Time
}

// getValue increases the total for a getValue counter
func (c *counter) incr(entry *types.CounterEntry) {
	c.countMutex.Lock()
	defer c.countMutex.Unlock()

	c.count += entry.Value
	c.lastUpdated = time.Now().UTC()
}

func (c *counter) getLastUpdated() time.Time {
	c.countMutex.RLock()
	defer c.countMutex.RUnlock()

	return c.lastUpdated
}

// getValue returns the total for a getValue counter
func (c *counter) getValue() int64 {
	c.countMutex.RLock()
	defer c.countMutex.RUnlock()

	return c.count
}

func (c *counter) getEntry() types.CounterEntry {
	e := c.entry
	e.Value = c.getValue()
	return *e
}

func compositeID(e *types.CounterEntry) string {
	labelVals := make([]string, 0)
	labelVals = append(labelVals, string(e.Name))
	for _, v := range e.Labels {
		labelVals = append(labelVals, v)
	}
	return strings.Join(labelVals, "-")
}

func (c *counter) reset() {
	c.countMutex.Lock()
	defer c.countMutex.Unlock()

	c.count = 0
}
