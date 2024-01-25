package rstreams

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/tunnel"
	"github.com/streamdal/plumber/validate"
)

func (r *RedisStreams) Tunnel(ctx context.Context, tunnelOpts *opts.TunnelOptions, tunnelSvc tunnel.ITunnel, errorCh chan<- *records.ErrorRecord) error {
	if err := validateTunnelOptions(tunnelOpts); err != nil {
		return errors.Wrap(err, "unable to validate tunnel options")
	}

	llog := r.log.WithField("pkg", "rstreams/tunnel")

	if err := tunnelSvc.Start(ctx, "Redis Streams", errorCh); err != nil {
		return errors.Wrap(err, "unable to create tunnel")
	}

	outboundCh := tunnelSvc.Read()

	for {
		select {
		case outbound := <-outboundCh:
			for _, streamName := range tunnelOpts.RedisStreams.Args.Streams {
				_, err := r.client.XAdd(ctx, &redis.XAddArgs{
					Stream: streamName,
					ID:     tunnelOpts.RedisStreams.Args.WriteId,
					Values: map[string]interface{}{
						tunnelOpts.RedisStreams.Args.Key: outbound.Blob,
					},
				}).Result()
				if err != nil {
					r.log.Errorf("unable to write message to '%s': %s", streamName, err)
					continue
				}

				r.log.Infof("Successfully wrote message to stream '%s' with key '%s' for replay '%s'",
					streamName, tunnelOpts.RedisStreams.Args.Key, outbound.ReplayId)
			}
		case <-ctx.Done():
			llog.Debug("context cancelled")
			return nil
		}
	}

	return nil
}

func validateTunnelOptions(tunnelOpts *opts.TunnelOptions) error {
	if tunnelOpts == nil {
		return validate.ErrEmptyTunnelOpts
	}

	if tunnelOpts.RedisStreams == nil {
		return validate.ErrEmptyBackendGroup
	}

	if tunnelOpts.RedisStreams.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if len(tunnelOpts.RedisStreams.Args.Streams) == 0 {
		return ErrMissingStream
	}

	return nil
}
