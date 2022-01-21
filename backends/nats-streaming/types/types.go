package types

import "github.com/nats-io/stan.go"

// RelayMessage encapsulates a NATS message that is read by relay.Run()
type RelayMessage struct {
	Value   *stan.Msg
	Options *RelayMessageOptions
}

// RelayMessageOptions contains any additional options necessary for processing of NATS messages by the relayer
type RelayMessageOptions struct {
	Channel string
}
