package types

import (
	"github.com/go-redis/redis/v8"
)

type RelayMessage struct {
	Value   *redis.Message
	Options *RelayMessageOptions
}

type RelayMessageOptions struct{}
