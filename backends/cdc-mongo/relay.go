package cdc_mongo

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/batchcorp/plumber/backends/cdc-mongo/types"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
	"github.com/batchcorp/plumber/stats"
)

type Relayer struct {
	Options         *cli.Options
	RelayCh         chan interface{}
	log             *logrus.Entry
	Service         *mongo.Client
	ShutdownContext context.Context
}

func Relay(opts *cli.Options, relayCh chan interface{}, shutdownCtx context.Context) (relay.IRelayBackend, error) {
	if err := validateRelayOptions(opts); err != nil {
		return nil, errors.Wrap(err, "unable to verify options")
	}

	// Create new service
	client, err := NewService(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create mongo connection")
	}

	return &Relayer{
		Options:         opts,
		RelayCh:         relayCh,
		Service:         client,
		ShutdownContext: shutdownCtx,
		log:             logrus.WithField("pkg", "cdc-mongo/relay.go"),
	}, nil
}

func (r *Relayer) Relay() error {
	var err error
	var cs *mongo.ChangeStream
	streamOpts := make([]*options.ChangeStreamOptions, 0)

	if r.Options.CDCMongo.IncludeFullDocument {
		streamOpts = append(streamOpts, options.ChangeStream().SetFullDocument(options.UpdateLookup))
	}

	if r.Options.CDCMongo.Database != "" {
		database := r.Service.Database(r.Options.CDCMongo.Database)
		if r.Options.CDCMongo.Collection == "" {
			// Watch specific database and all collections under it
			cs, err = database.Watch(r.ShutdownContext, mongo.Pipeline{}, streamOpts...)
		} else {
			// Watch specific database and collection deployment
			coll := database.Collection(r.Options.CDCMongo.Collection)
			cs, err = coll.Watch(r.ShutdownContext, mongo.Pipeline{}, streamOpts...)
		}
	} else {
		// Watch entire deployment
		cs, err = r.Service.Watch(r.ShutdownContext, mongo.Pipeline{}, streamOpts...)
	}

	if err != nil {
		return errors.Wrap(err, "could not begin change stream")
	}

	defer cs.Close(r.ShutdownContext)

	for {
		select {
		case <-r.ShutdownContext.Done():
			r.log.Info("Received shutdown signal, existing relayer")
			return nil
		default:
			// noop
		}

		// TODO: test this. Next blocks, but does the underlaying code handle the context correctly?
		if !cs.Next(r.ShutdownContext) {
			r.log.Errorf("unable to read message from mongo: %s", cs.Err())
		}

		stats.Incr("cdc-mongo-relay-consumer", 1)
		r.RelayCh <- &types.RelayMessage{
			Value: cs.Current,
		}
	}

	return nil
}

func validateRelayOptions(opts *cli.Options) error {
	if opts.CDCMongo.Collection != "" && opts.CDCMongo.Database == "" {
		return ErrMissingDatabase
	}
	return nil
}
