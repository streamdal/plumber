package cdc_mongo

import (
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/types"
	"github.com/pkg/errors"
)

func (c *CDCMongo) DisplayMessage(msg *types.ReadMessage) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	printer.PrintTable(nil, msg.Num, msg.ReceivedAt, msg.Value)

	return nil
}

func (c *CDCMongo) DisplayError(msg *types.ErrorMessage) error {
	printer.DefaultDisplayError(msg)
	return nil
}
