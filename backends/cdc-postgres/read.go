package cdc_postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/batchcorp/pgoutput"
	ptypes "github.com/batchcorp/plumber/backends/cdc-postgres/types"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
)

func (c *CDCPostgres) Read(ctx context.Context, resultsChan chan *types.ReadMessage, errorChan chan *types.ErrorMessage) error {
	if err := validateReadOptions(c.Options); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	set := pgoutput.NewRelationSet(nil)

	var changeRecord *ptypes.ChangeRecord

	sub := pgoutput.NewSubscription(c.service, c.Options.CDCPostgres.SlotName, c.Options.CDCPostgres.PublisherName,
		0, false)

	var count int

	handler := func(m pgoutput.Message, _ uint64) error {
		var err error
		var record *ptypes.ChangeRecord
		action := "unknown"

		switch v := m.(type) {
		case pgoutput.Begin:
			changeRecord = &ptypes.ChangeRecord{
				Timestamp: v.Timestamp.UTC().UnixNano(),
				XID:       v.XID,
				LSN:       pgx.FormatLSN(v.LSN),
			}
		case pgoutput.Commit:
			// Advance LSN so we do not read the same messages on re-connect
			sub.AdvanceLSN(v.LSN + 1)
		case pgoutput.Relation:
			set.Add(v)
		case pgoutput.Insert:
			action = "insert"
			record, err = handleInsert(set, &v, changeRecord)
		case pgoutput.Update:
			action = "update"
			record, err = handleUpdate(set, &v, changeRecord)
		case pgoutput.Delete:
			action = "delete"
			record, err = handleDelete(set, &v, changeRecord)
		}

		if err != nil {
			return fmt.Errorf("unable to handle '%s' record: %s", action, err)
		}

		output, err := json.MarshalIndent(record, "", "  ")
		if err != nil {
			return fmt.Errorf("unable to json marshal + indent '%s' record: %s", action, err)
		}

		count++

		resultsChan <- &types.ReadMessage{
			Value: output,
			Metadata: map[string]interface{}{
				"action": action,
			},
			ReceivedAt: time.Now().UTC(),
			Num:        count,
		}

		return nil
	}

	return sub.Start(ctx, 0, handler)
}

// validateReadOptions ensures the correct CLI options are specified for the read action
func validateReadOptions(opts *options.Options) error {
	return nil
}
