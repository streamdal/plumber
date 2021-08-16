package pulsar

import "github.com/batchcorp/plumber/types"

func (p *Pulsar) DisplayMessage(msg *types.ReadMessage) error {
	return nil
}

func (p *Pulsar) DisplayError(msg *types.ErrorMessage) error {
	return nil
}
