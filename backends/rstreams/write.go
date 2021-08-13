package rstreams

import (
	"context"
	"fmt"
	"strings"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

// Write is the entry point function for performing write operations in RedisStreams.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func (r *RedisStreams) Write(ctx context.Context, errorCh chan *types.ErrorMessage, messages ...*types.WriteMessage) error {
	if err := validateWriteOptions(r.Options); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	for _, msg := range messages {
		if err := r.write(ctx, msg.Value); err != nil {
			util.WriteError(r.log, errorCh, err)
		}
	}

	return nil
}

func (r *RedisStreams) write(ctx context.Context, value []byte) error {
	errs := make([]string, 0)

	for _, streamName := range r.Options.RedisStreams.Streams {
		_, err := r.client.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     r.Options.RedisStreams.WriteID,
			Values: map[string]interface{}{
				r.Options.RedisStreams.WriteKey: value,
			},
		}).Result()

		if err != nil {
			errs = append(errs, fmt.Sprintf("unable to write message to stream '%s': %s", streamName, err))
			continue
		}

		r.log.Infof("Successfully wrote message to stream '%s' with key '%s'",
			streamName, r.Options.RedisStreams.WriteKey)
	}

	if len(errs) != 0 {
		return errors.New(strings.Join(errs, "; "))
	}

	return nil
}

func validateWriteOptions(opts *options.Options) error {
	return nil
}
