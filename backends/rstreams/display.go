package rstreams

import "github.com/batchcorp/plumber/types"

func (r *RedisStreams) DisplayMessage(msg *types.ReadMessage) error {
	return nil
}

func (r *RedisStreams) DisplayError(msg *types.ErrorMessage) error {
	return nil
}
