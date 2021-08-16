package types

import (
	"time"

	"github.com/pkg/errors"
)

var (
	UnsupportedFeatureErr = errors.New("backend does not support this feature")
	NotImplementedErr     = errors.New("not implemented")
)

// WriteMessage is a generic message encoding used for writing messages to a backend
type WriteMessage struct {
	Value []byte
}

// ReadMessage is a generic encoding for messages that are read from a backend.
// The metadata field may be filled (or left unset) differently per backend.
type ReadMessage struct {
	Value    []byte
	Metadata map[string]interface{}

	ReceivedAt time.Time
	Num        int
}

type ErrorMessage struct {
	OccurredAt time.Time // UTC
	SourceIP   string
	Error      error
}

type TopicStats struct {
	TopicName  string
	GroupID    string
	Partitions map[int]*PartitionStats
}

type PartitionStats struct {
	MessagesBehind int64
}
