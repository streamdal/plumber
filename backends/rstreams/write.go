package rstreams

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber/util"

	"github.com/go-redis/redis/v8"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (r *RedisStreams) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	for _, value := range messages {
		for _, streamName := range writeOpts.RedisStreams.Args.Streams {
			_, err := r.client.XAdd(ctx, &redis.XAddArgs{
				Stream: streamName,
				ID:     writeOpts.RedisStreams.Args.WriteId,
				Values: map[string]interface{}{
					writeOpts.RedisStreams.Args.Key: value.Input,
				},
			}).Result()
			if err != nil {
				util.WriteError(nil, errorCh, fmt.Errorf("unable to write message to '%s': %s", streamName, err))
				continue
			}
		}
	}

	return nil
}

func validateWriteOptions(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return errors.New("write options cannot be nil")
	}

	if writeOpts.RedisStreams == nil {
		return errors.New("backend group options cannot be nil")
	}

	if writeOpts.RedisStreams.Args == nil {
		return errors.New("backend arg options cannot be nil")
	}

	if len(writeOpts.RedisStreams.Args.Streams) == 0 {
		return errors.New("you must specify at least one stream to write to")
	}

	return nil
}
