package cdc_mongo

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jhump/protoreflect/desc"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/printer"
)

func Read(opts *options.Options, _ *desc.MessageDescriptor) error {
	if err := validateReadOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	ctx := context.Background()

	service, err := NewService(opts)
	if err != nil {
		return errors.Wrap(err, "unable to connect to mongo server")
	}

	defer service.Disconnect(ctx)

	m := &CDCMongo{
		Options: opts,
		Context: ctx,
		Service: service,
		log:     logrus.WithField("pkg", "cdc-mongo/read.go"),
		printer: printer.New(),
	}

	return m.Read()
}

// Read attempts to read a change stream from mongo
func (m *CDCMongo) Read() error {

	var err error
	var cs *mongo.ChangeStream
	streamOpts := make([]*options.ChangeStreamOptions, 0)

	if m.Options.CDCMongo.IncludeFullDocument {
		streamOpts = append(streamOpts, options.ChangeStream().SetFullDocument(options.UpdateLookup))
	}

	if m.Options.CDCMongo.Database != "" {
		database := m.Service.Database(m.Options.CDCMongo.Database)
		if m.Options.CDCMongo.Collection == "" {
			// Watch specific database and all collections under it
			cs, err = database.Watch(m.Context, mongo.Pipeline{}, streamOpts...)
		} else {
			// Watch specific database and collection deployment
			coll := database.Collection(m.Options.CDCMongo.Collection)
			cs, err = coll.Watch(m.Context, mongo.Pipeline{}, streamOpts...)
		}
	} else {
		// Watch entire deployment
		cs, err = m.Service.Watch(m.Context, mongo.Pipeline{}, streamOpts...)
	}

	if err != nil {
		return errors.Wrap(err, "could not begin change stream")
	}
	defer cs.Close(m.Context)

	for {
		if !cs.Next(m.Context) {
			m.log.Errorf("unable to read message from mongo: %s", cs.Err())
			time.Sleep(time.Second * 1)
			continue
		}

		next := cs.Current.String()

		// Comes unformatted from mongo, let's make it nice for the end user
		tmp := make(map[string]interface{}, 0)
		if err := json.Unmarshal([]byte(next), &tmp); err != nil {
			return errors.Wrap(err, "unable to unmarshal JSON replication entry")
		}

		pretty, _ := json.MarshalIndent(tmp, "", "  ")
		printer.Print(string(pretty))

		if !m.Options.ReadFollow {
			return nil
		}
	}

	return nil
}

func validateReadOptions(opts *options.Options) error {
	if opts.CDCMongo.Collection != "" && opts.CDCMongo.Database == "" {
		return ErrMissingDatabase
	}
	return nil
}
