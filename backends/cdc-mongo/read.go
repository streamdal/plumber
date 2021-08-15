package cdc_mongo

import (
	"context"
	"encoding/json"
	"time"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	moptions "go.mongodb.org/mongo-driver/mongo/options"

	"github.com/batchcorp/plumber/options"
)

func (m *CDCMongo) Read(ctx context.Context, resultsChan chan *types.ReadMessage, errorChan chan *types.ErrorMessage) error {
	if err := validateReadOptions(m.Options); err != nil {
		return errors.Wrap(err, "unable to validate read options")
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

	var count int

	for {
		if !cs.Next(ctx) {
			util.WriteError(m.log, errorChan, errors.Wrap(cs.Err(), "unable to read message from mongo (retrying in 1s)"))
			time.Sleep(time.Second * 1)
			continue
		}

		next := cs.Current.String()

		// Comes unformatted from mongo, let's make it nice for the end user
		tmp := make(map[string]interface{}, 0)
		if err := json.Unmarshal([]byte(next), &tmp); err != nil {
			return errors.Wrap(err, "unable to unmarshal JSON replication entry")
		}

		count++

		pretty, _ := json.MarshalIndent(tmp, "", "  ")

		resultsChan <- &types.ReadMessage{
			Value:      pretty,
			ReceivedAt: time.Now().UTC(),
			Num:        count,
		}

		if !m.Options.Read.Follow {
			return nil
		}
	}
}

func validateReadOptions(opts *options.Options) error {
	if opts.CDCMongo.Collection != "" && opts.CDCMongo.Database == "" {
		return ErrMissingDatabase
	}
	return nil
}
