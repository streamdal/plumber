package types

import (
	"github.com/segmentio/kafka-go"
)

// RelayMessage encapsulates a kafka message that is read by relay.Run()
type RelayMessage struct {
	Value   *kafka.Message
	Options *RelayMessageOptions
}

// RelayMessageOptions contains any additional options necessary for processing of Kafka messages by the relayer
type RelayMessageOptions struct {
}

// OffsetInfo encapsulates info related to the offset count in the
type OffsetInfo struct {
	Count      int
	LastOffset int64
}
