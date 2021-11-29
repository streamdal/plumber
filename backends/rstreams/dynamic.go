package rstreams

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/dynamic"
	"github.com/batchcorp/plumber/validate"
)

func (r *RedisStreams) Dynamic(ctx context.Context, dynamicOpts *opts.DynamicOptions, dynamicSvc dynamic.IDynamic) error {
	if err := validateDynamicOptions(dynamicOpts); err != nil {
		return errors.Wrap(err, "unable to validate dynamic options")
	}

	llog := r.log.WithField("pkg", "rstreams/dynamic")

	go dynamicSvc.Start("Redis Streams")

	outboundCh := dynamicSvc.Read()

	for {
		select {
		case outbound := <-outboundCh:
			for _, streamName := range dynamicOpts.RedisStreams.Args.Stream {
				_, err := r.client.XAdd(ctx, &redis.XAddArgs{
					Stream: streamName,
					ID:     dynamicOpts.RedisStreams.Args.WriteId,
					Values: map[string]interface{}{
						dynamicOpts.RedisStreams.Args.Key: outbound.Blob,
					},
				}).Result()
				if err != nil {
					r.log.Errorf("unable to write message to '%s': %s", streamName, err)
					continue
				}

				r.log.Infof("Successfully wrote message to stream '%s' with key '%s' for replay '%s'",
					streamName, dynamicOpts.RedisStreams.Args.Key, outbound.ReplayId)
			}
		case <-ctx.Done():
			llog.Warning("context cancelled")
			return nil
		}
	}

	return nil
}

func validateDynamicOptions(dynamicOpts *opts.DynamicOptions) error {
	if dynamicOpts == nil {
		return validate.ErrEmptyDynamicOpts
	}

	if dynamicOpts.RedisStreams == nil {
		return validate.ErrEmptyBackendGroup
	}

	if dynamicOpts.RedisStreams.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if len(dynamicOpts.RedisStreams.Args.Stream) == 0 {
		return ErrMissingStream
	}

	return nil
}
