package nats_streaming

import "github.com/batchcorp/plumber/types"

func (n *NatsStreaming) DisplayMessage(msg *types.ReadMessage) error {
	return nil
}

func (n *NatsStreaming) DisplayError(msg *types.ErrorMessage) error {
	return nil
}
