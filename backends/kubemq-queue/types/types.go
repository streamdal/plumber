package types

import (
	"github.com/kubemq-io/kubemq-go/queues_stream"
)

type RelayMessage struct {
	Value   *queues_stream.QueueMessage
	Options *RelayMessageOptions
}

type RelayMessageOptions struct{}
