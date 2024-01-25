package cdcpostgres

import (
	"context"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"

	"github.com/streamdal/pgoutput"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/backends/cdcpostgres/types"
	"github.com/streamdal/plumber/prometheus"
	"github.com/streamdal/plumber/validate"
)

func (c *CDCPostgres) Relay(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}, errorCh chan<- *records.ErrorRecord) error {
	if err := validateRelayOptions(relayOpts); err != nil {
		return errors.Wrap(err, "unable to verify options")
	}

	args := relayOpts.Postgres.Args

	defer c.client.Close()
	set := pgoutput.NewRelationSet(nil)

	var changeRecord *types.ChangeRecord

	sub := pgoutput.NewSubscription(c.client, args.ReplicationSlotName, args.PublisherName, 0, false)

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

			prometheus.Incr("cdc-postgres-relay-consumer", 1)
			relayCh <- &types.RelayMessage{
				Value:   record,
				Options: &types.RelayMessageOptions{},
			}

		case pgoutput.Update:
			record, err := handleUpdate(set, &v, changeRecord)
			if err != nil {
				return err
			}

			prometheus.Incr("cdc-postgres-relay-consumer", 1)
			relayCh <- &types.RelayMessage{
				Value:   record,
				Options: &types.RelayMessageOptions{},
			}
		case pgoutput.Delete:
			record, err := handleDelete(set, &v, changeRecord)
			if err != nil {
				return err
			}

			prometheus.Incr("cdc-postgres-relay-consumer", 1)
			relayCh <- &types.RelayMessage{
				Value:   record,
				Options: &types.RelayMessageOptions{},
			}
		}
		return nil
	}

	err := sub.Start(ctx, 0, handler)
	if err == context.Canceled {
		c.log.Debug("Received shutdown signal, exiting relayer")
		return nil
	}

	return nil
}

// validateRelayOptions ensures all required relay options are present
func validateRelayOptions(relayOpts *opts.RelayOptions) error {
	if relayOpts == nil {
		return validate.ErrEmptyRelayOpts
	}

	if relayOpts.Postgres == nil {
		return validate.ErrEmptyBackendGroup
	}

	if relayOpts.Postgres.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	return nil
}
