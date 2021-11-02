package tstorage

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// A memoryPartition implements a partition to store data points on heap.
// It offers a goroutine safe capabilities.
type memoryPartition struct {
	// A hash map from metric name to memoryMetric.
	metrics sync.Map
	// The number of data points
	numPoints int64
	// minT is immutable.
	minT int64
	maxT int64

	// Write ahead log.
	wal wal
	// The timestamp range of partitions after which they get persisted
	partitionDuration  int64
	timestampPrecision TimestampPrecision
	once               sync.Once
}

func newMemoryPartition(wal wal, partitionDuration time.Duration, precision TimestampPrecision) partition {
	if wal == nil {
		wal = &nopWAL{}
	}
	var d int64
	switch precision {
	case Nanoseconds:
		d = partitionDuration.Nanoseconds()
	case Microseconds:
		d = partitionDuration.Microseconds()
	case Milliseconds:
		d = partitionDuration.Milliseconds()
	case Seconds:
		d = int64(partitionDuration.Seconds())
	default:
		d = partitionDuration.Nanoseconds()
	}
	return &memoryPartition{
		partitionDuration:  d,
		wal:                wal,
		timestampPrecision: precision,
	}
}

// insertRows inserts the given rows to partition.
func (m *memoryPartition) insertRows(rows []Row) ([]Row, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("no rows given")
	}
	// FIXME: Just emitting log is enough
	err := m.wal.append(operationInsert, rows)
	if err != nil {
		return nil, fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Set min timestamp at only first.
	m.once.Do(func() {
		min := rows[0].Timestamp
		for i := range rows {
			row := rows[i]
			if row.Timestamp < min {
				min = row.Timestamp
			}
		}
		atomic.StoreInt64(&m.minT, min)
	})

	outdatedRows := make([]Row, 0)
	maxTimestamp := rows[0].Timestamp
	var rowsNum int64
	for i := range rows {
		row := rows[i]
		if row.Timestamp < m.minTimestamp() {
			outdatedRows = append(outdatedRows, row)
			continue
		}
		if row.Timestamp == 0 {
			row.Timestamp = toUnix(time.Now(), m.timestampPrecision)
		}
		if row.Timestamp > maxTimestamp {
			maxTimestamp = row.Timestamp
		}
		name := marshalMetricName(row.Metric, row.Labels)
		mt := m.getMetric(name)
		mt.insertPoint(&row.DataPoint)
		rowsNum++
	}
	atomic.AddInt64(&m.numPoints, rowsNum)

	// Make max timestamp up-to-date.
	if atomic.LoadInt64(&m.maxT) < maxTimestamp {
		atomic.SwapInt64(&m.maxT, maxTimestamp)
	}

	return outdatedRows, nil
}

func toUnix(t time.Time, precision TimestampPrecision) int64 {
	switch precision {
	case Nanoseconds:
		return t.UnixNano()
	case Microseconds:
		return t.UnixNano() / 1e3
	case Milliseconds:
		return t.UnixNano() / 1e6
	case Seconds:
		return t.Unix()
	default:
		return t.UnixNano()
	}
}

func (m *memoryPartition) selectDataPoints(metric string, labels []Label, start, end int64) ([]*DataPoint, error) {
	name := marshalMetricName(metric, labels)
	mt := m.getMetric(name)
	return mt.selectPoints(start, end), nil
}

// getMetric gives back the reference to the metrics list whose name is the given one.
// If none, it creates a new one.
func (m *memoryPartition) getMetric(name string) *memoryMetric {
	value, ok := m.metrics.Load(name)
	if !ok {
		value = &memoryMetric{
			name:             name,
			points:           make([]*DataPoint, 0, 1000),
			outOfOrderPoints: make([]*DataPoint, 0),
		}
		m.metrics.Store(name, value)
	}
	return value.(*memoryMetric)
}

func (m *memoryPartition) minTimestamp() int64 {
	return atomic.LoadInt64(&m.minT)
}

func (m *memoryPartition) maxTimestamp() int64 {
	return atomic.LoadInt64(&m.maxT)
}

func (m *memoryPartition) size() int {
	return int(atomic.LoadInt64(&m.numPoints))
}

func (m *memoryPartition) active() bool {
	return m.maxTimestamp()-m.minTimestamp()+1 < m.partitionDuration
}

func (m *memoryPartition) clean() error {
	// What all data managed by memoryPartition is on heap that is automatically removed by GC.
	// So do nothing.
	return nil
}

func (m *memoryPartition) expired() bool {
	return false
}

// memoryMetric has a list of ordered data points that belong to the memoryMetric
type memoryMetric struct {
	name         string
	size         int64
	minTimestamp int64
	maxTimestamp int64
	// points must kept in order
	points           []*DataPoint
	outOfOrderPoints []*DataPoint
	mu               sync.RWMutex
}

func (m *memoryMetric) insertPoint(point *DataPoint) {
	size := atomic.LoadInt64(&m.size)
	// TODO: Consider to stop using mutex every time.
	//   Instead, fix the capacity of points slice, kind of like:
	/*
		m.points := make([]*DataPoint, 1000)
		for i := 0; i < 1000; i++ {
			m.points[i] = point
		}
	*/
	m.mu.Lock()
	defer m.mu.Unlock()

	// First insertion
	if size == 0 {
		m.points = append(m.points, point)
		atomic.StoreInt64(&m.minTimestamp, point.Timestamp)
		atomic.StoreInt64(&m.maxTimestamp, point.Timestamp)
		atomic.AddInt64(&m.size, 1)
		return
	}
	// Insert point in order
	if m.points[size-1].Timestamp < point.Timestamp {
		m.points = append(m.points, point)
		atomic.StoreInt64(&m.maxTimestamp, point.Timestamp)
		atomic.AddInt64(&m.size, 1)
		return
	}

	m.outOfOrderPoints = append(m.outOfOrderPoints, point)
}

// selectPoints returns a new slice by re-slicing with [startIdx:endIdx].
func (m *memoryMetric) selectPoints(start, end int64) []*DataPoint {
	size := atomic.LoadInt64(&m.size)
	minTimestamp := atomic.LoadInt64(&m.minTimestamp)
	maxTimestamp := atomic.LoadInt64(&m.maxTimestamp)
	var startIdx, endIdx int

	if end <= minTimestamp {
		return []*DataPoint{}
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	if start <= minTimestamp {
		startIdx = 0
	} else {
		// Use binary search because points are in-order.
		startIdx = sort.Search(int(size), func(i int) bool {
			return m.points[i].Timestamp >= start
		})
	}

	if end >= maxTimestamp {
		endIdx = int(size)
	} else {
		// Use binary search because points are in-order.
		endIdx = sort.Search(int(size), func(i int) bool {
			return m.points[i].Timestamp >= end
		})
	}
	return m.points[startIdx:endIdx]
}

// encodeAllPoints uses the given seriesEncoder to encode all metric data points in order by timestamp,
// including outOfOrderPoints.
func (m *memoryMetric) encodeAllPoints(encoder seriesEncoder) error {
	sort.Slice(m.outOfOrderPoints, func(i, j int) bool {
		return m.outOfOrderPoints[i].Timestamp < m.outOfOrderPoints[j].Timestamp
	})

	var oi, pi int
	for oi < len(m.outOfOrderPoints) && pi < len(m.points) {
		if m.outOfOrderPoints[oi].Timestamp < m.points[pi].Timestamp {
			if err := encoder.encodePoint(m.outOfOrderPoints[oi]); err != nil {
				return err
			}
			oi++
		} else {
			if err := encoder.encodePoint(m.points[pi]); err != nil {
				return err
			}
			pi++
		}
	}
	for oi < len(m.outOfOrderPoints) {
		if err := encoder.encodePoint(m.outOfOrderPoints[oi]); err != nil {
			return err
		}
		oi++
	}
	for pi < len(m.points) {
		if err := encoder.encodePoint(m.points[pi]); err != nil {
			return err
		}
		pi++
	}

	return nil
}
