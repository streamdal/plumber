package cdc_mongo

import "github.com/batchcorp/plumber/types"

func (c *CDCMongo) DisplayMessage(msg *types.ReadMessage) error {
	return nil
}

func (c *CDCMongo) DisplayError(msg *types.ErrorMessage) error {
	return nil
}
