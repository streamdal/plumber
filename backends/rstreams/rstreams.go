package rstreams

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
)

const (
	BackendName = "redis-streams"
)

type RedisStreams struct {
	Options *options.Options

	client *redis.Client
	log    *logrus.Entry
}

func New(opts *options.Options) (*RedisStreams, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	client, err := newClient(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new client")
	}

	return &RedisStreams{
		Options: opts,
		client:  client,
		log:     logrus.WithField("backend", "rstreams"),
	}, nil
}

func (r *RedisStreams) Name() string {
	return BackendName
}

func (r *RedisStreams) Close(ctx context.Context) error {
	if r.client == nil {
		return nil
	}

	if err := r.client.Close(); err != nil {
		return errors.Wrap(err, "unable to close client")
	}

	return nil
}

func (r *RedisStreams) Test(ctx context.Context) error {
	return types.NotImplementedErr
}

func (r *RedisStreams) Lag(ctx context.Context, resultsCh chan []*types.TopicStats, interval time.Duration) error {
	return types.UnsupportedFeatureErr
}

func newClient(opts *options.Options) (*redis.Client, error) {
	return redis.NewClient(&redis.Options{
		Addr:     opts.RedisPubSub.Address,
		Username: opts.RedisPubSub.Username,
		Password: opts.RedisPubSub.Password,
		DB:       opts.RedisPubSub.Database,
	}), nil
}

func createConsumerGroups(ctx context.Context, client *redis.Client, opts *options.RedisStreamsOptions) error {
	for _, stream := range opts.Streams {
		if opts.RecreateConsumerGroup {
			logrus.Debugf("deleting consumer group '%s'", opts.ConsumerGroup)

			_, err := client.XGroupDestroy(ctx, stream, opts.ConsumerGroup).Result()
			if err != nil {
				return fmt.Errorf("unable to recreate consumer group: %s", err)
			}
		}

		logrus.Debugf("Creating stream with start id '%s'", opts.StartID)

		var err error

		if opts.CreateStreams {
			_, err = client.XGroupCreateMkStream(ctx, stream, opts.ConsumerGroup, opts.StartID).Result()
		} else {
			_, err = client.XGroupCreate(ctx, stream, opts.ConsumerGroup, opts.StartID).Result()
		}

		if err != nil {
			// No problem if consumer group already exists
			if err.Error() != "BUSYGROUP Consumer Group name already exists" {
				return fmt.Errorf("error creating consumer group for stream '%s': %s", stream, err)
			}
		}
	}

	return nil
}

func validateOpts(opts *options.Options) error {
	if opts == nil {
		return errors.New("opts cannot be nil")
	}

	if opts.RedisStreams == nil {
		return errors.New("Redis Streams options cannot be nil")
	}

	return nil
}
