package rstreams

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
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
		return validate.ErrEmptyWriteOpts
	}

	if writeOpts.RedisStreams == nil {
		return validate.ErrEmptyBackendGroup
	}

	if writeOpts.RedisStreams.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if len(writeOpts.RedisStreams.Args.Streams) == 0 {
		return ErrMissingStream
	}

	return nil
}
