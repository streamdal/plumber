package nsq

import "github.com/batchcorp/plumber/types"

func (n *NSQ) DisplayMessage(msg *types.ReadMessage) error {
	return nil
}

func (n *NSQ) DisplayError(msg *types.ErrorMessage) error {
	return nil
}
