package cdc_postgres

import (
	"context"

	"github.com/jackc/pgx"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/pgoutput"
	ptypes "github.com/batchcorp/plumber/backends/cdc-postgres/types"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/stats"
	"github.com/batchcorp/plumber/types"
)

type Relayer struct {
	Options     *options.Options
	RelayCh     chan interface{}
	log         *logrus.Entry
	Service     *pgx.ReplicationConn
	ShutdownCtx context.Context
}

func (c *CDCPostgres) Relay(ctx context.Context, relayCh chan interface{}, errorCh chan *types.ErrorMessage) error {
	set := pgoutput.NewRelationSet(nil)

	var changeRecord *ptypes.ChangeRecord

	sub := pgoutput.NewSubscription(c.service, c.Options.CDCPostgres.SlotName, c.Options.CDCPostgres.PublisherName,
		0, false)

	handler := func(m pgoutput.Message, _ uint64) error {

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
			record, err := handleInsert(set, &v, changeRecord)
			if err != nil {
				return err
			}

			stats.Incr("cdc-postgres-relay-consumer", 1)
			relayCh <- &ptypes.RelayMessage{
				Value: record,
			}
		case pgoutput.Update:
			record, err := handleUpdate(set, &v, changeRecord)
			if err != nil {
				return err
			}

			stats.Incr("cdc-postgres-relay-consumer", 1)
			relayCh <- &ptypes.RelayMessage{
				Value: record,
			}
		case pgoutput.Delete:
			record, err := handleDelete(set, &v, changeRecord)
			if err != nil {
				return err
			}

			stats.Incr("cdc-postgres-relay-consumer", 1)
			relayCh <- &ptypes.RelayMessage{
				Value: record,
			}
		}

		return nil
	}

	// TODO: Use shutdown ctx instead of the user provided ctx
	// Start blocks. ShutdownCtx cancel is handled within the library
	err := sub.Start(ctx, 0, handler)
	if err == context.Canceled {
		c.log.Info("Received shutdown signal, existing relayer")
		return nil
	}

	return err
}
