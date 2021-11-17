package cdcmongo

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends/cdcmongo/types"
	"github.com/batchcorp/plumber/prometheus"
	"github.com/batchcorp/plumber/validate"
)

func (m *Mongo) Relay(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}, errorCh chan *records.ErrorRecord) error {
	if err := validateRelayOptions(relayOpts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	cs, err := m.getChangeStreamRelay(ctx, relayOpts)
	if err != nil {
		return err
	}

	defer cs.Close(ctx)

	for {
		if !cs.Next(ctx) {
			if cs.Err() == context.Canceled {
				m.log.Info("Received shutdown signal, existing relayer")
				return nil
			}

			m.log.Errorf("unable to read message from mongo: %s", cs.Err())
			prometheus.IncrPromCounter("plumber_read_errors", 1)
			time.Sleep(ReadRetryInterval)
			continue
		}

		relayCh <- &types.RelayMessage{
			Value:   cs.Current,
			Options: &types.RelayMessageOptions{},
		}

		prometheus.Incr("cdc-mongo-relay-consumer", 1)
	}

	defer cs.Close(ctx)

	return nil
}

func (m *Mongo) getChangeStreamRelay(ctx context.Context, readOpts *opts.RelayOptions) (*mongo.ChangeStream, error) {
	var err error
	var cs *mongo.ChangeStream
	streamOpts := make([]*options.ChangeStreamOptions, 0)

	if readOpts.Mongo.Args.IncludeFullDocument {
		streamOpts = append(streamOpts, options.ChangeStream().SetFullDocument(options.UpdateLookup))
	}

	if readOpts.Mongo.Args.Database != "" {
		database := m.client.Database(readOpts.Mongo.Args.Database)
		if readOpts.Mongo.Args.Collection == "" {
			// Watch specific database and all collections under it
			cs, err = database.Watch(ctx, mongo.Pipeline{}, streamOpts...)
		} else {
			// Watch specific database and collection deployment
			coll := database.Collection(readOpts.Mongo.Args.Collection)
			cs, err = coll.Watch(ctx, mongo.Pipeline{}, streamOpts...)
		}
	} else {
		// Watch entire deployment
		cs, err = m.client.Watch(ctx, mongo.Pipeline{}, streamOpts...)
	}

	if err != nil {
		return nil, errors.Wrap(err, "could not begin change stream")
	}

	return cs, nil
}

// validateRelayOptions ensures all required relay options are present
func validateRelayOptions(relayOpts *opts.RelayOptions) error {
	if relayOpts == nil {
		return validate.ErrEmptyRelayOpts
	}

	if relayOpts.Mongo == nil {
		return validate.ErrEmptyBackendGroup
	}

	if relayOpts.Mongo.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if relayOpts.Mongo.Args.Collection != "" && relayOpts.Mongo.Args.Database == "" {
		return ErrMissingDatabase
	}

	return nil
}
