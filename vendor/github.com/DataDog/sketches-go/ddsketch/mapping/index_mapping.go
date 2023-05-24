// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2020 Datadog, Inc.

package mapping

import (
	"fmt"
	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
)

const (
	expOverflow      = 7.094361393031e+02      // The value at which math.Exp overflows
	minNormalFloat64 = 2.2250738585072014e-308 //2^(-1022)
)

type IndexMapping interface {
	Equals(other IndexMapping) bool
	Index(value float64) int
	Value(index int) float64
	RelativeAccuracy() float64
	MinIndexableValue() float64
	MaxIndexableValue() float64
	ToProto() *sketchpb.IndexMapping
}

// FromProto returns an Index mapping from the protobuf definition of it
func FromProto(m *sketchpb.IndexMapping) (IndexMapping, error) {
	switch m.Interpolation {
	case sketchpb.IndexMapping_NONE:
		return NewLogarithmicMappingWithGamma(m.Gamma, m.IndexOffset)
	case sketchpb.IndexMapping_LINEAR:
		return NewLinearlyInterpolatedMappingWithGamma(m.Gamma, m.IndexOffset)
	case sketchpb.IndexMapping_CUBIC:
		return NewCubicallyInterpolatedMappingWithGamma(m.Gamma, m.IndexOffset)
	default:
		return nil, fmt.Errorf("interpolation not supported: %d", m.Interpolation)
	}
}
