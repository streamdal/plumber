package cdc_mongo

import (
	"context"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/printer"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(opts.CDCMongo.DSN))
	if err != nil {
		return nil, errors.Wrap(err, "could not open mongo connection")
	}

	return client, nil
}
