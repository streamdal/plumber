package tstorage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/nakabonne/tstorage/internal/syscall"
)

const (
	dataFileName = "data"
	metaFileName = "meta.json"
)

var (
	errInvalidPartition = errors.New("invalid partition")
)

// A disk partition implements a partition that uses local disk as a storage.
// It mainly has two files, data file and meta file.
// The data file is memory-mapped and read only; no need to lock at all.
type diskPartition struct {
	dirPath string
	meta    meta
	// file descriptor of data file
	f *os.File
	// memory-mapped file backed by f
	mappedFile []byte
	// duration to store data
	retention time.Duration
}

// meta is a mapper for a meta file, which is put for each partition.
// Note that the CreatedAt is surely timestamped by tstorage but Min/Max Timestamps are likely to do by other process.
type meta struct {
	MinTimestamp  int64                 `json:"minTimestamp"`
	MaxTimestamp  int64                 `json:"maxTimestamp"`
	NumDataPoints int                   `json:"numDataPoints"`
	Metrics       map[string]diskMetric `json:"metrics"`
	CreatedAt     time.Time             `json:"createdAt"`
}

// diskMetric holds meta data to access actual data from the memory-mapped file.
type diskMetric struct {
	Name          string `json:"name"`
	Offset        int64  `json:"offset"`
	MinTimestamp  int64  `json:"minTimestamp"`
	MaxTimestamp  int64  `json:"maxTimestamp"`
	NumDataPoints int64  `json:"numDataPoints"`
}

// openDiskPartition first maps the data file into memory with memory-mapping.
func openDiskPartition(dirPath string, retention time.Duration) (partition, error) {
	if dirPath == "" {
		return nil, fmt.Errorf("dir path is required")
	}
	metaFilePath := filepath.Join(dirPath, metaFileName)
	_, err := os.Stat(metaFilePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, errInvalidPartition
	}

	// Map data to the memory
	dataPath := filepath.Join(dirPath, dataFileName)
	f, err := os.Open(dataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read data file: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch file info: %w", err)
	}
	if info.Size() == 0 {
		return nil, ErrNoDataPoints
	}
	mapped, err := syscall.Mmap(int(f.Fd()), int(info.Size()))
	if err != nil {
		return nil, fmt.Errorf("failed to perform mmap: %w", err)
	}

	// Read metadata to the heap
	m := meta{}
	mf, err := os.Open(metaFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}
	defer mf.Close()
	decoder := json.NewDecoder(mf)
	if err := decoder.Decode(&m); err != nil {
		return nil, fmt.Errorf("failed to decode metadata: %w", err)
	}
	return &diskPartition{
		dirPath:    dirPath,
		meta:       m,
		f:          f,
		mappedFile: mapped,
		retention:  retention,
	}, nil
}

func (d *diskPartition) insertRows(_ []Row) ([]Row, error) {
	return nil, fmt.Errorf("can't insert rows into disk partition")
}

func (d *diskPartition) selectDataPoints(metric string, labels []Label, start, end int64) ([]*DataPoint, error) {
	if d.expired() {
		return nil, fmt.Errorf("this partition is expired: %w", ErrNoDataPoints)
	}
	name := marshalMetricName(metric, labels)
	mt, ok := d.meta.Metrics[name]
	if !ok {
		return nil, ErrNoDataPoints
	}
	r := bytes.NewReader(d.mappedFile)
	if _, err := r.Seek(mt.Offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek: %w", err)
	}
	decoder, err := newSeriesDecoder(r)
	if err != nil {
		return nil, fmt.Errorf("failed to generate decoder for metric %q in %q: %w", name, d.dirPath, err)
	}

	// TODO: Divide fixed-lengh chunks when flushing, and index it.
	points := make([]*DataPoint, 0, mt.NumDataPoints)
	for i := 0; i < int(mt.NumDataPoints); i++ {
		point := &DataPoint{}
		if err := decoder.decodePoint(point); err != nil {
			return nil, fmt.Errorf("failed to decode point of metric %q in %q: %w", name, d.dirPath, err)
		}
		if point.Timestamp < start {
			continue
		}
		if point.Timestamp >= end {
			break
		}
		points = append(points, point)
	}
	return points, nil
}

func (d *diskPartition) minTimestamp() int64 {
	return d.meta.MinTimestamp
}

func (d *diskPartition) maxTimestamp() int64 {
	return d.meta.MaxTimestamp
}

func (d *diskPartition) size() int {
	return d.meta.NumDataPoints
}

// Disk partition is immutable.
func (d *diskPartition) active() bool {
	return false
}

func (d *diskPartition) clean() error {
	if err := os.RemoveAll(d.dirPath); err != nil {
		return fmt.Errorf("failed to remove all files inside the partition (%d~%d): %w", d.minTimestamp(), d.maxTimestamp(), err)
	}

	return nil
}

func (d *diskPartition) expired() bool {
	diff := time.Since(d.meta.CreatedAt)
	if diff > d.retention {
		return true
	}
	return false
}
