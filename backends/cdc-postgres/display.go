package cdc_postgres

import (
	"fmt"

	ptypes "github.com/batchcorp/plumber/backends/cdc-postgres/types"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/types"
	"github.com/pkg/errors"
)

func (c *CDCPostgres) DisplayMessage(msg *types.ReadMessage) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	rawMsg, ok := msg.Raw.(*ptypes.ChangeRecord)
	if !ok {
		return errors.New("unable to type assert message")
	}

	properties := [][]string{
		{"LSN", rawMsg.LSN},
		{"Table", rawMsg.Table},
		{"Operation", rawMsg.Operation},
		{"XID", fmt.Sprint(rawMsg.XID)},
	}

	printer.PrintTable(properties, msg.Num, msg.ReceivedAt, msg.Value)

	return nil
}

func (c *CDCPostgres) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}
