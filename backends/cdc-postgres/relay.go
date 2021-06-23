package cdc_postgres

import (
	"context"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/pgoutput"
	"github.com/batchcorp/plumber/backends/cdc-postgres/types"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
	"github.com/batchcorp/plumber/stats"
)

type Relayer struct {
	Options     *cli.Options
	RelayCh     chan interface{}
	log         *logrus.Entry
	Service     *pgx.ReplicationConn
	ShutdownCtx context.Context
}

func Relay(opts *cli.Options, relayCh chan interface{}, shutdownCtx context.Context) (relay.IRelayBackend, error) {
	// Create new service
	client, err := NewService(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create postgres connection")
	}

	return &Relayer{
		Options:     opts,
		RelayCh:     relayCh,
		Service:     client,
		ShutdownCtx: shutdownCtx,
		log:         logrus.WithField("pkg", "cdc-postgres/relay.go"),
	}, nil
}

func (r *Relayer) Relay() error {
	set := pgoutput.NewRelationSet(nil)

	var changeRecord *types.ChangeRecord

	sub := pgoutput.NewSubscription(r.Service, r.Options.CDCPostgres.SlotName, r.Options.CDCPostgres.PublisherName, 0, false)
	defer r.Service.Close()

	handler := func(m pgoutput.Message, _ uint64) error {

		switch v := m.(type) {
		case pgoutput.Begin:
			changeRecord = &types.ChangeRecord{
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
			r.RelayCh <- &types.RelayMessage{
				Value: record,
			}
		case pgoutput.Update:
			record, err := handleUpdate(set, &v, changeRecord)
			if err != nil {
				return err
			}

			stats.Incr("cdc-postgres-relay-consumer", 1)
			r.RelayCh <- &types.RelayMessage{
				Value: record,
			}
		case pgoutput.Delete:
			record, err := handleDelete(set, &v, changeRecord)
			if err != nil {
				return err
			}

			stats.Incr("cdc-postgres-relay-consumer", 1)
			r.RelayCh <- &types.RelayMessage{
				Value: record,
			}
		}

		return nil
	}

	// Start blocks. ShutdownCtx cancel is handled within the library
	err := sub.Start(r.ShutdownCtx, 0, handler)
	if err == context.Canceled {
		r.log.Info("Received shutdown signal, existing relayer")
		return nil
	}

	return err
}
