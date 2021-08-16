package awssqs

import "github.com/batchcorp/plumber/types"

func (a *AWSSQS) DisplayMessage(msg *types.ReadMessage) error {
	return nil
}

func (a *AWSSQS) DisplayError(msg *types.ErrorMessage) error {
	return nil
}
