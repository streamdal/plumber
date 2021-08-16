package azure

import "github.com/batchcorp/plumber/types"

func (s *ServiceBus) DisplayMessage(msg *types.ReadMessage) error {
	return nil
}

func (s *ServiceBus) DisplayError(msg *types.ErrorMessage) error {
	return nil
}
