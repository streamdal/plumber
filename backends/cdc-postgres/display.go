package cdc_postgres

import "github.com/batchcorp/plumber/types"

func (c *CDCPostgres) DisplayMessage(msg *types.ReadMessage) error {
	return nil
}

func (c *CDCPostgres) DisplayError(msg *types.ErrorMessage) error {
	return nil
}
