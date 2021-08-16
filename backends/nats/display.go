package nats

import "github.com/batchcorp/plumber/types"

func (n *Nats) DisplayMessage(msg *types.ReadMessage) error {
	return nil
}

func (n *Nats) DisplayError(msg *types.ErrorMessage) error {
	return nil
}
