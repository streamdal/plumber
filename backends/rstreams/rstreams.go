package rstreams

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/jhump/protoreflect/desc"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
)

type RedisStreams struct {
	Options *cli.Options
	Client  *redis.Client
	MsgDesc *desc.MessageDescriptor
	Context context.Context
	log     *logrus.Entry
}

func NewClient(opts *cli.Options) (*redis.Client, error) {
	return redis.NewClient(&redis.Options{
		Addr:     opts.Redis.Address,
		Username: opts.Redis.Username,
		Password: opts.Redis.Password,
		DB:       opts.Redis.Database,
	}), nil
}

func NewStreamsClient(opts *cli.Options) (*redis.Client, error) {
	return redis.NewClient(&redis.Options{
		Addr:     opts.RedisStreams.Address,
		Username: opts.RedisStreams.Username,
		Password: opts.RedisStreams.Password,
		DB:       opts.RedisStreams.Database,
	}), nil
}

func CreateConsumerGroups(ctx context.Context, client *redis.Client, opts *cli.RedisStreamsOptions) error {
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
