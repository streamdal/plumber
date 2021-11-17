package types

import mqtt "github.com/eclipse/paho.mqtt.golang"

// RelayMessage encapsulates a MQTT message
type RelayMessage struct {
	Value   mqtt.Message
	Options *RelayMessageOptions
}

// RelayMessageOptions contains any additional options necessary for processing of the messages by the relayer
type RelayMessageOptions struct {
}
