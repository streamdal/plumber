package kafka

import "github.com/batchcorp/plumber/types"

func (k *Kafka) DisplayMessage(msg *types.ReadMessage) error {
	return nil
}

func (k *Kafka) DisplayError(msg *types.ErrorMessage) error {
	return nil
}
