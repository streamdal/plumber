package mqtt

import "github.com/batchcorp/plumber/types"

func (m *MQTT) DisplayMessage(msg *types.ReadMessage) error {
	return nil
}

func (m *MQTT) DisplayError(msg *types.ErrorMessage) error {
	return nil
}
