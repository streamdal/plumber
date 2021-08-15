package types

import (
	"time"

	"github.com/pkg/errors"
)

var (
	BackendNotConnectedErr = errors.New("backend not connected")
	UnsupportedFeatureErr  = errors.New("backend does not support this feature")
	NotImplementedErr      = errors.New("not implemeneted")
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

// TODO: Implement
type Lag struct {
	Topic           string
	ConsumerGroupID string
}

type PartitionStats struct {
	PartitionID    string
	MessagesBehind int
}
