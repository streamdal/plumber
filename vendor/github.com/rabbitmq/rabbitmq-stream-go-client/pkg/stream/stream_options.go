package stream

import (
	"fmt"
	"time"
)

type StreamOptions struct {
	MaxAge              time.Duration
	MaxLengthBytes      *ByteCapacity
	MaxSegmentSizeBytes *ByteCapacity
}

func (s *StreamOptions) SetMaxAge(maxAge time.Duration) *StreamOptions {
	s.MaxAge = maxAge
	return s
}

func (s *StreamOptions) SetMaxLengthBytes(maxLength *ByteCapacity) *StreamOptions {
	s.MaxLengthBytes = maxLength
	return s
}

func (s *StreamOptions) SetMaxSegmentSizeBytes(segmentSize *ByteCapacity) *StreamOptions {
	s.MaxSegmentSizeBytes = segmentSize
	return s
}

func (s StreamOptions) buildParameters() (map[string]string, error) {
	res := map[string]string{"queue-leader-locator": "least-leaders"}

	if s.MaxLengthBytes != nil {
		if s.MaxLengthBytes.error != nil {
			return nil, s.MaxLengthBytes.error
		}

		if s.MaxLengthBytes.bytes > 0 {
			res["max-length-bytes"] = fmt.Sprintf("%d", s.MaxLengthBytes.bytes)
		}
	}

	if s.MaxSegmentSizeBytes != nil {
		if s.MaxSegmentSizeBytes.error != nil {
			return nil, s.MaxSegmentSizeBytes.error
		}

		if s.MaxSegmentSizeBytes.bytes > 0 {
			res["stream-max-segment-size-bytes"] = fmt.Sprintf("%d", s.MaxSegmentSizeBytes.bytes)
		}
	}

	if s.MaxAge > 0 {
		res["max-age"] = fmt.Sprintf("%.0fs", s.MaxAge.Seconds())
	}
	return res, nil
}

func NewStreamOptions() *StreamOptions {
	return &StreamOptions{}
}
