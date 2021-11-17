package types

import (
	"cloud.google.com/go/pubsub"
)

type RelayMessage struct {
	Value   *pubsub.Message
	Options *RelayMessageOptions
}

type RelayMessageOptions struct {
}
