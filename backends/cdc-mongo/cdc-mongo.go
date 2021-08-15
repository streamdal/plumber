package cdc_mongo

import (
	"context"
	"time"

	"github.com/batchcorp/plumber/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	moptions "go.mongodb.org/mongo-driver/mongo/options"

	"github.com/batchcorp/plumber/options"
)

const (
	// ConnectionTimeout determines how long before a connection attempt to mongo is timed out
	ConnectionTimeout = time.Second * 10

	// ReadRetryInterval is how long to wait between read errors before plumber tries reading again
	ReadRetryInterval = time.Second * 5
)

var (
	ErrMissingDatabase  = errors.New("you must specify the --database flag")
	ErrConnectionFailed = errors.New("could not open mongo connection")
)

type CDCMongo struct {
	Id      string
	Service *mongo.Client
	Context context.Context
	Options *options.Options
	log     *logrus.Entry
}

func New(opts *options.Options) (*CDCMongo, error) {
	if err := validateOptions(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	service, err := newService(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new service")
	}

	return &CDCMongo{
		Id:      "foo",
		Service: service,
		Options: opts,
		log:     logrus.WithField("pkg", "cdc-mongo"),
	}, nil
}

func (m *CDCMongo) Close(ctx context.Context) error {
	if m.Service == nil {
		return nil
	}

	if err := m.Service.Disconnect(ctx); err != nil {
		return errors.Wrap(err, "unable to disconnect from mongo")
	}

	return nil
}

func (m *CDCMongo) Write(ctx context.Context, errorCh chan *types.ErrorMessage, messages ...*types.WriteMessage) error {
	return types.UnsupportedFeatureErr
}

func (m *CDCMongo) Test(ctx context.Context) error {
	return types.NotImplementedErr
}

func (m *CDCMongo) Dynamic(ctx context.Context) error {
	return types.UnsupportedFeatureErr
}

func (m *CDCMongo) Lag(ctx context.Context, resultsCh chan []*types.TopicStats, interval time.Duration) error {
	return types.UnsupportedFeatureErr
}

func newService(opts *options.Options) (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ConnectionTimeout)
	defer cancel()

	client, err := mongo.Connect(ctx, moptions.Client().ApplyURI(opts.CDCMongo.DSN))
	if err != nil {
		return nil, errors.Wrap(err, ErrConnectionFailed.Error())
	}

	return client, nil
}

func validateOptions(opts *options.Options) error {
	if opts == nil {
		return errors.New("opts cannot be nil")
	}

	if opts.CDCMongo == nil {
		return errors.New("mongo options cannot be nil")
	}

	return nil
}
