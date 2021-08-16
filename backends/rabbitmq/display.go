package rabbitmq

import "github.com/batchcorp/plumber/types"

func (r *RabbitMQ) DisplayMessage(msg *types.ReadMessage) error {
	return nil
}

func (r *RabbitMQ) DisplayError(msg *types.ErrorMessage) error {
	return nil
}
