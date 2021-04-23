package cdc_postgres

import (
	"context"

	"github.com/batchcorp/pgoutput"
	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/backends/cdc-postgres/types"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
	"github.com/batchcorp/plumber/stats"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Relayer struct {
	Options        *cli.Options
	RelayCh        chan interface{}
	log            *logrus.Entry
	Service        *pgx.ReplicationConn
	DefaultContext context.Context
}

func Relay(opts *cli.Options) error {

	// Create new relayer instance (+ validate token & gRPC address)
	relayCfg := &relay.Config{
		Token:       opts.RelayToken,
		GRPCAddress: opts.RelayGRPCAddress,
		NumWorkers:  opts.RelayNumWorkers,
		Timeout:     opts.RelayGRPCTimeout,
		RelayCh:     make(chan interface{}, 1),
		DisableTLS:  opts.RelayGRPCDisableTLS,
		Type:        opts.RelayType,
	}

	grpcRelayer, err := relay.New(relayCfg)
	if err != nil {
		return errors.Wrap(err, "unable to create new gRPC relayer")
	}

	// Create new service
	client, err := NewService(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create postgres connection")
	}

	// Launch HTTP server
	go func() {
		if err := api.Start(opts.RelayHTTPListenAddress, opts.Version); err != nil {
			logrus.Fatalf("unable to start API server: %s", err)
		}
	}()

	// Launch gRPC Relayer
	if err := grpcRelayer.StartWorkers(); err != nil {
		return errors.Wrap(err, "unable to start gRPC relay workers")
	}

	r := &Relayer{
		Options: opts,
		RelayCh: relayCfg.RelayCh,
		Service: client,
		log:     logrus.WithField("pkg", "cdc-postgres/relay.go"),
	}

	return r.Relay()
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
	return sub.Start(context.Background(), 0, handler)
}
