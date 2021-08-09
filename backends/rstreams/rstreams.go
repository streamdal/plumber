package rstreams

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/jhump/protoreflect/desc"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
)

type RedisStreams struct {
	Options *options.Options
	Client  *redis.Client
	MsgDesc *desc.MessageDescriptor
	Context context.Context
	log     *logrus.Entry
}

func NewClient(opts *options.Options) (*redis.Client, error) {
	return redis.NewClient(&redis.Options{
		Addr:     opts.RedisPubSub.Address,
		Username: opts.RedisPubSub.Username,
		Password: opts.RedisPubSub.Password,
		DB:       opts.RedisPubSub.Database,
	}), nil
}

func NewStreamsClient(opts *options.Options) (*redis.Client, error) {
	return redis.NewClient(&redis.Options{
		Addr:     opts.RedisStreams.Address,
		Username: opts.RedisStreams.Username,
		Password: opts.RedisStreams.Password,
		DB:       opts.RedisStreams.Database,
	}), nil
}

func CreateConsumerGroups(ctx context.Context, client *redis.Client, opts *options.RedisStreamsOptions) error {
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
