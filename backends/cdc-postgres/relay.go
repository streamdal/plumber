package cdc_postgres

import (
	"context"
	"fmt"
	"github.com/batchcorp/pgoutput"
	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/backends/cdc-postgres/types"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
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
		log:     logrus.WithField("pkg", "azure/relay.go"),
	}

	return r.Relay()
}

func (r *Relayer) Relay() error {
	set := pgoutput.NewRelationSet(nil)

	var changeRecord *types.ChangeRecord
	changeRecord = &types.ChangeRecord{}
	changeRecord.Changes = make([]*types.Change, 0)

	sub := pgoutput.NewSubscription(r.Service, r.Options.CDCPostgres.SlotName, r.Options.CDCPostgres.PublisherName, 0, false)
	defer r.Service.Close()

	handler := func(m pgoutput.Message, _ uint64) error {

		switch v := m.(type) {
		case pgoutput.Begin:
			changeRecord.Timestamp = v.Timestamp.UTC().UnixNano()
		case pgoutput.Commit:
			changeRecord.LSN = pgx.FormatLSN(v.LSN)

			// Advance LSN so we do not read the same messages on re-connect
			sub.AdvanceLSN(v.LSN + 1)

			r.RelayCh <- &types.RelayMessage{
				Value: changeRecord,
			}

			changeRecord = &types.ChangeRecord{}
			changeRecord.Changes = make([]*types.Change, 0)
		case pgoutput.Relation:
			set.Add(v)
		case pgoutput.Insert:
			changeSet, ok := set.Get(v.RelationID)
			if !ok {
				return errors.New("relation not found")
			}
			values, err := set.Values(v.RelationID, v.Row)
			if err != nil {
				return fmt.Errorf("error parsing values: %s", err)
			}

			change := &types.Change{
				Operation: "insert",
				Table:     changeSet.Name,
				Fields:    make(map[string]interface{}, 0),
			}
			for name, value := range values {
				change.Fields[name] = value.Get()
			}
			changeRecord.Changes = append(changeRecord.Changes, change)
		case pgoutput.Update:
			changeSet, ok := set.Get(v.RelationID)
			if !ok {
				return errors.New("relation not found")
			}
			values, err := set.Values(v.RelationID, v.Row)
			if err != nil {
				return fmt.Errorf("error parsing values: %s", err)
			}

			change := &types.Change{
				Operation: "update",
				Table:     changeSet.Name,
				Fields:    make(map[string]interface{}, 0),
			}
			for name, value := range values {
				change.Fields[name] = value.Get()
			}
			changeRecord.Changes = append(changeRecord.Changes, change)
		case pgoutput.Delete:
			changeSet, ok := set.Get(v.RelationID)
			if !ok {
				err := fmt.Errorf("relation not found for '%s'", changeSet.Name)
				r.log.Error(err)
				return err
			}
			values, err := set.Values(v.RelationID, v.Row)
			if err != nil {
				values = make(map[string]pgtype.Value, 0)
				r.log.Debugf("Error parsing value: %s", err)
			}

			change := &types.Change{
				Operation: "delete",
				Table:     changeSet.Name,
				Fields:    make(map[string]interface{}, 0),
			}
			for name, value := range values {
				val := value.Get()

				// Deletes will include the primary key with the rest of the fields being empty. Ignore them
				if val == "" {
					continue
				}

				change.Fields[name] = val
			}
			changeRecord.Changes = append(changeRecord.Changes, change)
		}

		return nil
	}
	return sub.Start(context.Background(), 0, handler)
}
