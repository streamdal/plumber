package types

import (
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

// RelayMessage encapsulates a kafka message that is read by relay.Run()
type RelayMessage struct {
	Value   *azservicebus.ReceivedMessage
	Options *RelayMessageOptions
}

// RelayMessageOptions contains any additional options necessary for processing of Kafka messages by the relayer
type RelayMessageOptions struct {
}
