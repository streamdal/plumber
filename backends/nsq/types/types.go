package types

import (
	"github.com/nsqio/go-nsq"
)

// RelayMessage encapsulates a NSQ message that is read by relay.Run()
type RelayMessage struct {
	Value   *nsq.Message
	Options *RelayMessageOptions
}

// RelayMessageOptions contains any additional options necessary for processing of NSQ messages by the relayer
type RelayMessageOptions struct {
	Topic   string
	Channel string
}
