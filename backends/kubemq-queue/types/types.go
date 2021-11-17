package types

import (
	queuesStream "github.com/kubemq-io/kubemq-go/queues_stream"
)

type RelayMessage struct {
	Value   *queuesStream.QueueMessage
	Options *RelayMessageOptions
}

type RelayMessageOptions struct{}
