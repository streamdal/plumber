package activemq

import "github.com/batchcorp/plumber/types"

func (a *ActiveMq) DisplayMessage(msg *types.ReadMessage) error {
	return nil
}

func (a *ActiveMq) DisplayError(msg *types.ErrorMessage) error {
	return nil
}
