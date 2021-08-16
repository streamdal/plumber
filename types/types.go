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
	// Value contains the actual payload of the message
	Value []byte

	// Metadata may contain properties that cannot be found in the Raw message.
	// For example: read lag in Kafka. This varies for each backend; look at
	// the specific backend Read() method to see what fields are available.
	Metadata map[string]interface{}

	// Raw is the original, untouched message; accessed usually by Display*
	// methods of the specific backend.
	Raw interface{}

	// ReceivedAt is a UTC timestamp of when plumber received the message
	ReceivedAt time.Time

	// Num is an incremental number that plumber assigns to each message it
	// receives. This is mostly useful for CLI functionality to allow the user
	// to quickly discern whether this is message #1 or #500, etc.
	Num int
}

type ErrorMessage struct {
	OccurredAt time.Time // UTC
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
