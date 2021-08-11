package types

import (
	"time"

	"github.com/pkg/errors"
)

var (
	BackendNotConnectedErr     = errors.New("backend not connected")
	BackendAlreadyConnectedErr = errors.New("backend already connected")
)

type Message struct {
	ReceivedAt time.Time // UTC
	MessageNum int
	Value      interface{}
}

type ErrorMessage struct {
	OccurredAt time.Time // UTC
	SourceIP   string
	Error      error
}

// TODO: Implement
type LagStats struct {
}
