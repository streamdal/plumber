package rpubsub

import (
	"context"

	"github.com/batchcorp/plumber/types"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
)

type Redis struct {
	Options *options.Options

	client *redis.Client
	log    *logrus.Entry
}

func New(opts *options.Options) (*Redis, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	client, err := newClient(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new client")
	}

	return &Redis{
		Options: opts,
		client:  client,
		log:     logrus.WithField("backend", "rpubsub"),
	}, nil
}

func (r *Redis) Close(ctx context.Context) error {
	if r.client == nil {
		return nil
	}

	if err := r.client.Close(); err != nil {
		return errors.Wrap(err, "unable to close client")
	}

	return nil
}

func (r *Redis) Test(ctx context.Context) error {
	return types.NotImplementedErr
}

func (r *Redis) Lag(ctx context.Context) (*types.Lag, error) {
	return nil, types.UnsupportedFeatureErr
}

func newClient(opts *options.Options) (*redis.Client, error) {
	return redis.NewClient(&redis.Options{
		Addr:     opts.RedisPubSub.Address,
		Username: opts.RedisPubSub.Username,
		Password: opts.RedisPubSub.Password,
		DB:       opts.RedisPubSub.Database,
	}), nil
}

func validateOpts(opts *options.Options) error {
	if opts == nil {
		return errors.New("opts cannot be nil")
	}

	if opts.RedisPubSub == nil {
		return errors.New("Redis PubSub options cannot be nil")
	}

	return nil
}
