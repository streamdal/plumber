package cdc_mongo

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/printer"
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
	Options *cli.Options
	log     *logrus.Entry
	printer printer.IPrinter
}

func NewService(opts *cli.Options) (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ConnectionTimeout)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(opts.CDCMongo.DSN))
	if err != nil {
		return nil, errors.Wrap(err, ErrConnectionFailed.Error())
	}

	return client, nil
}
