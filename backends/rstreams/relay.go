package rstreams

import (
	"context"
	"fmt"
	"time"

	"github.com/batchcorp/plumber/validate"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/backends/rstreams/types"

	"github.com/batchcorp/plumber/prometheus"
)

// RetryReadInterval determines how long to wait before retrying a read, after an error has occurred
const RetryReadInterval = 5 * time.Second

func (r *RedisStreams) Relay(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}, errorCh chan *records.ErrorRecord) error {
	if err := validateRelayOptions(relayOpts); err != nil {
		return errors.Wrap(err, "unable to validate relay options")
	}

	for {
		streamsResult, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    relayOpts.RedisStreams.Args.ConsumerGroup,
			Consumer: relayOpts.RedisStreams.Args.ConsumerName,
			Streams:  generateStreams(relayOpts.RedisStreams.Args.Streams),
			Count:    int64(relayOpts.RedisStreams.Args.Count),
			Block:    0,
			NoAck:    false,
		}).Result()

		if err != nil {
			if err == context.Canceled {
				r.log.Debug("Received shutdown signal, exiting relayer")
				return nil
			}

			errorCh <- &records.ErrorRecord{
				OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
				Error:               errors.Wrap(err, "unable to read message from redis-streams").Error(),
			}

			// Temporarily mute stats
			prometheus.Mute("redis-streams-relay-consumer")
			prometheus.Mute("redis-streams-relay-producer")

			prometheus.IncrPromCounter("plumber_read_errors", 1)

			r.log.Errorf("Unable to read message(s): %s (retrying in %s)", err, RetryReadInterval)

			time.Sleep(RetryReadInterval)

			continue
		}

		// We may be reading from multiple streamsResult - read each stream resp
		for _, stream := range streamsResult {
			streamName := stream.Stream

			// Each stream result may contain multiple messages
			for _, message := range stream.Messages {
				// A single message may contain multiple kv's
				for k, v := range message.Values {
					stringData, ok := v.(string)
					if !ok {
						errorCh <- &records.ErrorRecord{
							OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
							Error: fmt.Sprintf("[ID: %s Stream: %s Key: %s] unable to type assert value as string: %s; skipping",
								message.ID, streamName, k, v),
						}
						continue
					}

					prometheus.Incr("redis-streams-relay-consumer", 1)

					// Generate relay message
					relayCh <- &types.RelayMessage{
						ID:     message.ID,
						Key:    k,
						Stream: streamName,
						Value:  []byte(stringData),
					}

					r.log.Debugf("[ID: %s Stream: %s Key: %s] successfully relayed message", message.ID, streamName, k)
				}
			}
		}
	}
}

// validateRelayOptions ensures all required relay options are present
func validateRelayOptions(relayOpts *opts.RelayOptions) error {
	if relayOpts == nil {
		return validate.ErrEmptyRelayOpts
	}

	if relayOpts.RedisStreams == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := relayOpts.RedisStreams.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if len(args.Streams) == 0 {
		return ErrMissingStream
	}

	return nil
}
