package types

import mqtt "github.com/eclipse/paho.mqtt.golang"

// RelayMessage encapsulates a kafka message that is read by relay.Run()
type RelayMessage struct {
	Value   mqtt.Message
	Options *RelayMessageOptions
}

// RelayMessageOptions contains any additional options necessary for processing of Kafka messages by the relayer
type RelayMessageOptions struct {
}
