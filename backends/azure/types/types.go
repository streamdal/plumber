package types

import servicebus "github.com/Azure/azure-service-bus-go"

// RelayMessage encapsulates a kafka message that is read by relay.Run()
type RelayMessage struct {
	Value   *servicebus.Message
	Options *RelayMessageOptions
}

// RelayMessageOptions contains any additional options necessary for processing of Kafka messages by the relayer
type RelayMessageOptions struct {
}
