package cdcmongo

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (m *Mongo) Read(ctx context.Context, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord) error {
	if err := validateReadOptions(readOpts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	cs, err := m.getChangeStream(ctx, readOpts)
	if err != nil {
		return err
	}

	defer cs.Close(ctx)

	var count int64

	m.log.Info("Waiting for CDC events...")

	for {
		if !cs.Next(ctx) {
			if errors.Is(cs.Err(), context.Canceled) {
				return nil
			}
			m.log.Errorf("unable to read message from mongo: %s", cs.Err())
			time.Sleep(ReadRetryInterval)
			continue
		}

		count++

		msg := cs.Current.String()

		serializedMsg, err := json.Marshal(msg)
		if err != nil {
			return errors.Wrap(err, "unable to serialize msg to JSON")
		}

		ts := time.Now().UTC().Unix()

		resultsChan <- &records.ReadRecord{
			MessageId:           uuid.NewV4().String(),
			Num:                 count,
			Metadata:            nil,
			ReceivedAtUnixTsUtc: ts,
			Payload:             []byte(msg),
			XRaw:                serializedMsg,
			Record: &records.ReadRecord_Mongo{
				Mongo: &records.Mongo{
					Value:     []byte(msg),
					Timestamp: ts,
				},
			},
		}

		if !readOpts.Continuous {
			return nil
		}
	}

	return nil
}

func (m *Mongo) getChangeStream(ctx context.Context, readOpts *opts.ReadOptions) (*mongo.ChangeStream, error) {
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

func validateReadOptions(readOpts *opts.ReadOptions) error {
	if readOpts.Mongo == nil {
		return errors.New("Mongo read options cannot be nil")
	}

	if readOpts.Mongo.Args == nil {
		return errors.New("Mongo read option args cannot be nil")
	}

	if readOpts.Mongo.Args.Collection != "" && readOpts.Mongo.Args.Database == "" {
		return ErrMissingDatabase
	}

	return nil
}
