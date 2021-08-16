package rabbitmq_streams

import "github.com/batchcorp/plumber/types"

func (r *RabbitMQStreams) DisplayMessage(msg *types.ReadMessage) error {
	return nil
}

func (r *RabbitMQStreams) DisplayError(msg *types.ErrorMessage) error {
	return nil
}
