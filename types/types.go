package types

import (
	"github.com/pkg/errors"
)

var (
	UnsupportedFeatureErr = errors.New("backend does not support this feature")
	NotImplementedErr     = errors.New("not implemented")
)

type TopicStats struct {
	TopicName  string
	GroupID    string
	Partitions map[int]*PartitionStats
}

type PartitionStats struct {
	MessagesBehind int64
}
