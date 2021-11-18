package stomp

import (
	"github.com/go-stomp/stomp/frame"
)

// The AckMode type is an enumeration of the acknowledgement modes for a
// STOMP subscription.
type AckMode int

// String returns the string representation of the AckMode value.
func (a AckMode) String() string {
	switch a {
	case AckAuto:
		return frame.AckAuto
	case AckClient:
		return frame.AckClient
	case AckClientIndividual:
		return frame.AckClientIndividual
	}
	panic("invalid AckMode value")
}

// ShouldAck returns true if this AckMode is an acknowledgement
// mode which requires acknowledgement. Returns true for all values
// except AckAuto, which returns false.
func (a AckMode) ShouldAck() bool {
	switch a {
	case AckAuto:
		return false
	case AckClient, AckClientIndividual:
		return true
	}
	panic("invalid AckMode value")
}

const (
	// No acknowledgement is required, the server assumes that the client
	// received the message.
	AckAuto AckMode = iota

	// Client acknowledges messages. When a client acknowledges a message,
	// any previously received messages are also acknowledged.
	AckClient

	// Client acknowledges message. Each message is acknowledged individually.
	AckClientIndividual
)
