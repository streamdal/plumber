package cdcmongo

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/types"
)

const BackendName = "cdc-mongo"

type Mongo struct {
	// Base connection options / non-backend-specific options
	connOpts *opts.ConnectionOptions

	// Backend-specific args
	connArgs *args.MongoConn

	client *mongo.Client
	log    *logrus.Entry
}

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

func New(connOpts *opts.ConnectionOptions) (*Mongo, error) {
	if err := validateBaseConnOpts(connOpts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	ctx, cancel := context.WithTimeout(context.Background(), ConnectionTimeout)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connOpts.GetMongo().Dsn))
	if err != nil {
		return nil, errors.Wrap(err, ErrConnectionFailed.Error())
	}

	return &Mongo{
		client:   client,
		connOpts: connOpts,
		connArgs: connOpts.GetMongo(),
		log:      logrus.WithField("backend", BackendName),
	}, nil
}

func (m *Mongo) Name() string {
	return BackendName
}

func (m *Mongo) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}

func (m *Mongo) Test(_ context.Context) error {
	return types.NotImplementedErr
}

func validateBaseConnOpts(connOpts *opts.ConnectionOptions) error {
	if connOpts == nil {
		return errors.New("connection config cannot be nil")
	}

	if connOpts.Conn == nil {
		return errors.New("connection object in connection config cannot be nil")
	}

	if connOpts.GetMongo() == nil {
		return errors.New("connection config args cannot be nil")
	}

	return nil
}
