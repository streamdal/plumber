package awssns

import (
	"github.com/batchcorp/plumber/types"
)

func (a *AWSSNS) DisplayMessage(msg *types.ReadMessage) error {
	return types.UnsupportedFeatureErr
}

func (a *AWSSNS) DisplayError(msg *types.ErrorMessage) error {
	return types.UnsupportedFeatureErr

}