// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2020 Datadog, Inc.

package store

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
)

const (
	arrayLengthOverhead        = 64
	arrayLengthGrowthIncrement = 0.1

	// Grow the bins with an extra growthBuffer bins to prevent growing too often
	growthBuffer = 128
)

// DenseStore is a dynamically growing contiguous (non-sparse) store. The number of bins are
// bound only by the size of the slice that can be allocated.
type DenseStore struct {
	bins     []float64
	count    float64
	offset   int
	minIndex int
	maxIndex int
}

func NewDenseStore() *DenseStore {
	return &DenseStore{minIndex: math.MaxInt32, maxIndex: math.MinInt32}
}

func (s *DenseStore) Add(index int) {
	s.AddWithCount(index, float64(1))
}

func (s *DenseStore) AddBin(bin Bin) {
	index := bin.Index()
	count := bin.Count()
	if count == 0 {
		return
	}
	s.AddWithCount(index, count)
}

func (s *DenseStore) AddWithCount(index int, count float64) {
	if count == 0 {
		return
	}
	arrayIndex := s.normalize(index)
	s.bins[arrayIndex] += count
	s.count += count
}

// Normalize the store, if necessary, so that the counter of the specified index can be updated.
func (s *DenseStore) normalize(index int) int {
	if index < s.minIndex || index > s.maxIndex {
		s.extendRange(index, index)
	}
	return index - s.offset
}

func (s *DenseStore) getNewLength(newMinIndex, newMaxIndex int) int {
	desiredLength := newMaxIndex - newMinIndex + 1
	return int((float64(desiredLength+arrayLengthOverhead-1)/arrayLengthGrowthIncrement + 1) * arrayLengthGrowthIncrement)
}

func (s *DenseStore) extendRange(newMinIndex, newMaxIndex int) {

	newMinIndex = min(newMinIndex, s.minIndex)
	newMaxIndex = max(newMaxIndex, s.maxIndex)

	if s.IsEmpty() {
		initialLength := s.getNewLength(newMinIndex, newMaxIndex)
		s.bins = make([]float64, initialLength)
		s.offset = newMinIndex
		s.minIndex = newMinIndex
		s.maxIndex = newMaxIndex
		s.adjust(newMinIndex, newMaxIndex)
	} else if newMinIndex >= s.offset && newMaxIndex < s.offset+len(s.bins) {
		s.minIndex = newMinIndex
		s.maxIndex = newMaxIndex
	} else {
		// To avoid shifting too often when nearing the capacity of the array,
		// we may grow it before we actually reach the capacity.
		newLength := s.getNewLength(newMinIndex, newMaxIndex)
		if newLength > len(s.bins) {
			tmpBins := make([]float64, newLength)
			copy(tmpBins, s.bins)
			s.bins = tmpBins
		}
		s.adjust(newMinIndex, newMaxIndex)
	}
}

// Adjust bins, offset, minIndex and maxIndex, without resizing the bins slice in order to make it fit the
// specified range.
func (s *DenseStore) adjust(newMinIndex, newMaxIndex int) {
	s.centerCounts(newMinIndex, newMaxIndex)
}

func (s *DenseStore) centerCounts(newMinIndex, newMaxIndex int) {
	midIndex := newMinIndex + (newMaxIndex-newMinIndex+1)/2
	s.shiftCounts(s.offset + len(s.bins)/2 - midIndex)
	s.minIndex = newMinIndex
	s.maxIndex = newMaxIndex
}

func (s *DenseStore) shiftCounts(shift int) {
	minArrIndex := s.minIndex - s.offset
	maxArrIndex := s.maxIndex - s.offset
	copy(s.bins[minArrIndex+shift:], s.bins[minArrIndex:maxArrIndex+1])
	if shift > 0 {
		s.resetBins(s.minIndex, s.minIndex+shift-1)
	} else {
		s.resetBins(s.maxIndex+shift+1, s.maxIndex)
	}
	s.offset -= shift
}

func (s *DenseStore) resetBins(fromIndex, toIndex int) {
	for i := fromIndex - s.offset; i <= toIndex-s.offset; i++ {
		s.bins[i] = 0
	}
}

func (s *DenseStore) IsEmpty() bool {
	return s.count == 0
}

func (s *DenseStore) TotalCount() float64 {
	return s.count
}

func (s *DenseStore) MinIndex() (int, error) {
	if s.IsEmpty() {
		return 0, errors.New("MinIndex of empty store is undefined.")
	}
	return s.minIndex, nil
}

func (s *DenseStore) MaxIndex() (int, error) {
	if s.IsEmpty() {
		return 0, errors.New("MaxIndex of empty store is undefined.")
	}
	return s.maxIndex, nil
}

// Return the key for the value at rank
func (s *DenseStore) KeyAtRank(rank float64) int {
	var n float64
	for i, b := range s.bins {
		n += b
		if n > rank {
			return i + s.offset
		}
	}
	return s.maxIndex
}

func (s *DenseStore) MergeWith(other Store) {
	if other.IsEmpty() {
		return
	}
	o, ok := other.(*DenseStore)
	if !ok {
		for bin := range other.Bins() {
			s.AddBin(bin)
		}
		return
	}
	if o.minIndex < s.minIndex || o.maxIndex > s.maxIndex {
		s.extendRange(o.minIndex, o.maxIndex)
	}
	for idx := o.minIndex; idx <= o.maxIndex; idx++ {
		s.bins[idx-s.offset] += o.bins[idx-o.offset]
	}
	s.count += o.count
}

func (s *DenseStore) Bins() <-chan Bin {
	ch := make(chan Bin)
	go func() {
		defer close(ch)
		for idx := s.minIndex; idx <= s.maxIndex; idx++ {
			if s.bins[idx-s.offset] > 0 {
				ch <- Bin{index: idx, count: s.bins[idx-s.offset]}
			}
		}
	}()
	return ch
}

func (s *DenseStore) Copy() Store {
	bins := make([]float64, len(s.bins))
	copy(bins, s.bins)
	return &DenseStore{
		bins:     bins,
		count:    s.count,
		offset:   s.offset,
		minIndex: s.minIndex,
		maxIndex: s.maxIndex,
	}
}

func (s *DenseStore) string() string {
	var buffer bytes.Buffer
	buffer.WriteString("{")
	for i := 0; i < len(s.bins); i++ {
		index := i + s.offset
		buffer.WriteString(fmt.Sprintf("%d: %f, ", index, s.bins[i]))
	}
	buffer.WriteString(fmt.Sprintf("count: %v, offset: %d, minIndex: %d, maxIndex: %d}", s.count, s.offset, s.minIndex, s.maxIndex))
	return buffer.String()
}

func (s *DenseStore) ToProto() *sketchpb.Store {
	if s.IsEmpty() {
		return &sketchpb.Store{ContiguousBinCounts: nil}
	}
	bins := make([]float64, s.maxIndex-s.minIndex+1)
	copy(bins, s.bins[s.minIndex-s.offset:s.maxIndex-s.offset+1])
	return &sketchpb.Store{
		ContiguousBinCounts:      bins,
		ContiguousBinIndexOffset: int32(s.minIndex),
	}
}
