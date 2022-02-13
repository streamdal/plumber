package rpubsub

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends/rpubsub/types"
	"github.com/batchcorp/plumber/prometheus"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

const (
	RetryReadInterval = 5 * time.Second
)

func (r *RedisPubsub) Relay(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}, errorCh chan<- *records.ErrorRecord) error {
	if err := validateRelayOptions(relayOpts); err != nil {
		return errors.Wrap(err, "invalid relay options")
	}

	ps := r.client.Subscribe(ctx, relayOpts.RedisPubsub.Args.Channels...)
	defer ps.Unsubscribe(ctx)

	for {
		msg, err := ps.ReceiveMessage(ctx)
		if err != nil {
			if strings.Contains(err.Error(), "operation was canceled") {
				r.log.Debug("Received shutdown signal, exiting relayer")
				return nil
			}

			// This will happen every loop when the context times out
			if strings.Contains(err.Error(), "i/o timeout") {
				time.Sleep(time.Millisecond * 100)
				continue
			}

			// Temporarily mute stats
			prometheus.Mute("redis-pubsub-relay-consumer")
			prometheus.Mute("redis-pubsub-relay-producer")

			prometheus.IncrPromCounter("plumber_read_errors", 1)

			util.WriteError(r.log, errorCh, fmt.Errorf("unable to read message: %s (retrying in %s)", err, RetryReadInterval.String()))

			time.Sleep(RetryReadInterval)
			continue
		}

		prometheus.Incr("redis-pubsub-relay-consumer", 1)
		relayCh <- &types.RelayMessage{
			Value:   msg,
			Options: &types.RelayMessageOptions{},
		}
	}

	return nil
}

func validateRelayOptions(relayOpts *opts.RelayOptions) error {
	if relayOpts == nil {
		return validate.ErrEmptyRelayOpts
	}

	if relayOpts.RedisPubsub == nil {
		return validate.ErrEmptyBackendGroup
	}

	if relayOpts.RedisPubsub.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if len(relayOpts.RedisPubsub.Args.Channels) == 0 {
		return ErrMissingChannel
	}

	return nil
}
