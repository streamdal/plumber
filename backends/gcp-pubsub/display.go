package gcppubsub

import "github.com/batchcorp/plumber/types"

func (g *GCPPubSub) DisplayMessage(msg *types.ReadMessage) error {
	return nil
}

func (g *GCPPubSub) DisplayError(msg *types.ErrorMessage) error {
	return nil
}
