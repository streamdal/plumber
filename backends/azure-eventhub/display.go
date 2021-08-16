package azure_eventhub

import "github.com/batchcorp/plumber/types"

func (e *EventHub) DisplayMessage(msg *types.ReadMessage) error {
	return nil
}

func (e *EventHub) DisplayError(msg *types.ErrorMessage) error {
	return nil
}
