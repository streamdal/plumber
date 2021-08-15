package cdc_mongo

import (
	"context"
	"time"

	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	moptions "go.mongodb.org/mongo-driver/mongo/options"

	mtypes "github.com/batchcorp/plumber/backends/cdc-mongo/types"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/stats"
	"github.com/batchcorp/plumber/types"
)

type Relayer struct {
	Options     *options.Options
	RelayCh     chan interface{}
	log         *logrus.Entry
	Service     *mongo.Client
	ShutdownCtx context.Context
}

func (m *CDCMongo) Relay(ctx context.Context, relayCh chan interface{}, errorCh chan *types.ErrorMessage) error {
	if err := validateRelayOptions(m.Options); err != nil {
		return errors.Wrap(err, "unable to verify options")
	}

	var err error
	var cs *mongo.ChangeStream

	streamOpts := make([]*moptions.ChangeStreamOptions, 0)

	if m.Options.CDCMongo.IncludeFullDocument {
		streamOpts = append(streamOpts, moptions.ChangeStream().SetFullDocument(moptions.UpdateLookup))
	}

	if m.Options.CDCMongo.Database != "" {
		database := m.Service.Database(m.Options.CDCMongo.Database)
		if m.Options.CDCMongo.Collection == "" {
			// Watch specific database and all collections under it
			cs, err = database.Watch(ctx, mongo.Pipeline{}, streamOpts...)
		} else {
			// Watch specific database and collection deployment
			coll := database.Collection(m.Options.CDCMongo.Collection)
			cs, err = coll.Watch(ctx, mongo.Pipeline{}, streamOpts...)
		}
	} else {
		// Watch entire deployment
		cs, err = m.Service.Watch(ctx, mongo.Pipeline{}, streamOpts...)
	}

	if err != nil {
		return errors.Wrap(err, "could not begin change stream")
	}

	defer cs.Close(ctx)

	for {
		if !cs.Next(ctx) {
			if cs.Err() == context.Canceled {
				m.log.Info("Received shutdown signal, existing relayer")
				return nil
			}

			stats.Mute("redis-pubsub-relay-consumer")
			stats.Mute("redis-pubsub-relay-producer")

			stats.IncrPromCounter("plumber_read_errors", 1)

			util.WriteError(m.log, errorCh, errors.Wrap(err, "unable to read message from mongo"))

			time.Sleep(ReadRetryInterval)
			continue
		}

		stats.Incr("cdc-mongo-relay-consumer", 1)
		relayCh <- &mtypes.RelayMessage{
			Value: cs.Current,
		}
	}
}

func validateRelayOptions(opts *options.Options) error {
	if opts.CDCMongo.Collection != "" && opts.CDCMongo.Database == "" {
		return ErrMissingDatabase
	}

	return nil
}
