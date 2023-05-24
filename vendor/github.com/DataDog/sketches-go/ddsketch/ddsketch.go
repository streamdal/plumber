// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2020 Datadog, Inc.

package ddsketch

import (
	"errors"
	"math"

	"github.com/DataDog/sketches-go/ddsketch/mapping"
	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
	"github.com/DataDog/sketches-go/ddsketch/store"
)

type DDSketch struct {
	mapping.IndexMapping
	positiveValueStore store.Store
	negativeValueStore store.Store
	zeroCount          float64
}

func NewDDSketch(indexMapping mapping.IndexMapping, positiveValueStore store.Store, negativeValueStore store.Store) *DDSketch {
	return &DDSketch{
		IndexMapping:       indexMapping,
		positiveValueStore: positiveValueStore,
		negativeValueStore: negativeValueStore,
	}
}

func NewDefaultDDSketch(relativeAccuracy float64) (*DDSketch, error) {
	return LogUnboundedDenseDDSketch(relativeAccuracy)
}

// Constructs an instance of DDSketch that offers constant-time insertion and whose size grows indefinitely
// to accommodate for the range of input values.
func LogUnboundedDenseDDSketch(relativeAccuracy float64) (*DDSketch, error) {
	indexMapping, err := mapping.NewLogarithmicMapping(relativeAccuracy)
	if err != nil {
		return nil, err
	}
	return NewDDSketch(indexMapping, store.NewDenseStore(), store.NewDenseStore()), nil
}

// Constructs an instance of DDSketch that offers constant-time insertion and whose size grows until the
// maximum number of bins is reached, at which point bins with lowest indices are collapsed, which causes the
// relative accuracy guarantee to be lost on lowest quantiles if values are all positive, or the mid-range
// quantiles for values closest to zero if values include negative numbers.
func LogCollapsingLowestDenseDDSketch(relativeAccuracy float64, maxNumBins int) (*DDSketch, error) {
	indexMapping, err := mapping.NewLogarithmicMapping(relativeAccuracy)
	if err != nil {
		return nil, err
	}
	return NewDDSketch(indexMapping, store.NewCollapsingLowestDenseStore(maxNumBins), store.NewCollapsingLowestDenseStore(maxNumBins)), nil
}

// Constructs an instance of DDSketch that offers constant-time insertion and whose size grows until the
// maximum number of bins is reached, at which point bins with highest indices are collapsed, which causes the
// relative accuracy guarantee to be lost on highest quantiles if values are all positive, or the lowest and
// highest quantiles if values include negative numbers.
func LogCollapsingHighestDenseDDSketch(relativeAccuracy float64, maxNumBins int) (*DDSketch, error) {
	indexMapping, err := mapping.NewLogarithmicMapping(relativeAccuracy)
	if err != nil {
		return nil, err
	}
	return NewDDSketch(indexMapping, store.NewCollapsingHighestDenseStore(maxNumBins), store.NewCollapsingHighestDenseStore(maxNumBins)), nil
}

// Adds a value to the sketch.
func (s *DDSketch) Add(value float64) error {
	return s.AddWithCount(value, float64(1))
}

// Adds a value to the sketch with a float64 count.
func (s *DDSketch) AddWithCount(value, count float64) error {
	if value < -s.MaxIndexableValue() || value > s.MaxIndexableValue() {
		return errors.New("The input value is outside the range that is tracked by the sketch.")
	}
	if count < 0 {
		return errors.New("The count cannot be negative.")
	}

	if value > s.MinIndexableValue() {
		s.positiveValueStore.AddWithCount(s.Index(value), count)
	} else if value < -s.MinIndexableValue() {
		s.negativeValueStore.AddWithCount(s.Index(-value), count)
	} else {
		s.zeroCount += count
	}
	return nil
}

// Return a (deep) copy of this sketch.
func (s *DDSketch) Copy() *DDSketch {
	return &DDSketch{
		IndexMapping:       s.IndexMapping,
		positiveValueStore: s.positiveValueStore.Copy(),
		negativeValueStore: s.negativeValueStore.Copy(),
	}
}

// Return the value at the specified quantile. Return a non-nil error if the quantile is invalid
// or if the sketch is empty.
func (s *DDSketch) GetValueAtQuantile(quantile float64) (float64, error) {
	if quantile < 0 || quantile > 1 {
		return math.NaN(), errors.New("The quantile must be between 0 and 1.")
	}

	count := s.GetCount()
	if count == 0 {
		return math.NaN(), errors.New("No such element exists")
	}

	rank := quantile * (count - 1)
	negativeValueCount := s.negativeValueStore.TotalCount()
	if rank < negativeValueCount {
		return -s.Value(s.negativeValueStore.KeyAtRank(negativeValueCount - 1 - rank)), nil
	} else if rank < s.zeroCount+negativeValueCount {
		return 0, nil
	} else {
		return s.Value(s.positiveValueStore.KeyAtRank(rank - s.zeroCount - negativeValueCount)), nil
	}
}

// Return the values at the respective specified quantiles. Return a non-nil error if any of the quantiles
// is invalid or if the sketch is empty.
func (s *DDSketch) GetValuesAtQuantiles(quantiles []float64) ([]float64, error) {
	values := make([]float64, len(quantiles))
	for i, q := range quantiles {
		val, err := s.GetValueAtQuantile(q)
		if err != nil {
			return nil, err
		}
		values[i] = val
	}
	return values, nil
}

// Return the total number of values that have been added to this sketch.
func (s *DDSketch) GetCount() float64 {
	return s.zeroCount + s.positiveValueStore.TotalCount() + s.negativeValueStore.TotalCount()
}

// Return true iff no value has been added to this sketch.
func (s *DDSketch) IsEmpty() bool {
	return s.zeroCount == 0 && s.positiveValueStore.IsEmpty() && s.negativeValueStore.IsEmpty()
}

// Return the maximum value that has been added to this sketch. Return a non-nil error if the sketch
// is empty.
func (s *DDSketch) GetMaxValue() (float64, error) {
	if !s.positiveValueStore.IsEmpty() {
		maxIndex, _ := s.positiveValueStore.MaxIndex()
		return s.Value(maxIndex), nil
	} else if s.zeroCount > 0 {
		return 0, nil
	} else {
		minIndex, err := s.negativeValueStore.MinIndex()
		if err != nil {
			return math.NaN(), err
		}
		return -s.Value(minIndex), nil
	}
}

// Return the minimum value that has been added to this sketch. Returns a non-nil error if the sketch
// is empty.
func (s *DDSketch) GetMinValue() (float64, error) {
	if !s.negativeValueStore.IsEmpty() {
		maxIndex, _ := s.negativeValueStore.MaxIndex()
		return -s.Value(maxIndex), nil
	} else if s.zeroCount > 0 {
		return 0, nil
	} else {
		minIndex, err := s.positiveValueStore.MinIndex()
		if err != nil {
			return math.NaN(), err
		}
		return s.Value(minIndex), nil
	}
}

// Merges the other sketch into this one. After this operation, this sketch encodes the values that
// were added to both this and the other sketches.
func (s *DDSketch) MergeWith(other *DDSketch) error {
	if !s.IndexMapping.Equals(other.IndexMapping) {
		return errors.New("Cannot merge sketches with different index mappings.")
	}
	s.positiveValueStore.MergeWith(other.positiveValueStore)
	s.negativeValueStore.MergeWith(other.negativeValueStore)
	s.zeroCount += other.zeroCount
	return nil
}

// Generates a protobuf representation of this DDSketch.
func (s *DDSketch) ToProto() *sketchpb.DDSketch {
	return &sketchpb.DDSketch{
		Mapping:        s.IndexMapping.ToProto(),
		PositiveValues: s.positiveValueStore.ToProto(),
		NegativeValues: s.negativeValueStore.ToProto(),
		ZeroCount:      s.zeroCount,
	}
}

// Builds a new instance of DDSketch based on the provided protobuf representation.
func (s *DDSketch) FromProto(pb *sketchpb.DDSketch) (*DDSketch, error) {
	m, err := mapping.FromProto(pb.Mapping)
	if err != nil {
		return nil, err
	}
	return &DDSketch{
		IndexMapping:       m,
		positiveValueStore: store.FromProto(pb.PositiveValues),
		negativeValueStore: store.FromProto(pb.NegativeValues),
		zeroCount:          pb.ZeroCount,
	}, nil
}
