package rpubsub

import "github.com/batchcorp/plumber/types"

func (r *Redis) DisplayMessage(msg *types.ReadMessage) error {
	return nil
}

func (r *Redis) DisplayError(msg *types.ErrorMessage) error {
	return nil
}
