package types

import (
	"go.mongodb.org/mongo-driver/bson"
)

// RelayMessage encapsulates a mongodb change stream message that is read by relay.Run()
type RelayMessage struct {
	Value   bson.Raw
	Options *RelayMessageOptions
}

// RelayMessageOptions contains any additional options necessary for processing of mongo messages by the relayer
type RelayMessageOptions struct {
}
