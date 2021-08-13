package rstreams

import (
	"context"
	"fmt"
	"time"

	"github.com/batchcorp/plumber/util"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"

	rtypes "github.com/batchcorp/plumber/backends/rstreams/types"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/stats"
	"github.com/batchcorp/plumber/types"
)

const (
	RetryReadInterval = 5 * time.Second
)

type Relayer struct {
	Client      *redis.Client
	Options     *options.Options
	RelayCh     chan interface{}
	log         *logrus.Entry
	Looper      *director.FreeLooper
	ShutdownCtx context.Context
}

var (
	errMissingStream = errors.New("You must specify at least one stream")
)

// Relay reads messages from RedisStreams and sends them to RelayCh which is then read by relay.Run()
func (r *RedisStreams) Relay(ctx context.Context, relayCh chan interface{}, errorCh chan *types.ErrorMessage) error {
	if err := validateRelayOptions(r.Options); err != nil {
		return errors.Wrap(err, "unable to verify options")
	}

	// Create consumer group (and stream) for each stream
	if err := createConsumerGroups(ctx, r.client, r.Options.RedisStreams); err != nil {
		return fmt.Errorf("unable to create consumer group(s): %s", err)
	}

	r.log.Infof("Relaying RedisStreams messages from %d stream(s) (%s) -> '%s'",
		len(r.Options.RedisStreams.Streams), r.Options.RedisStreams.Streams, r.Options.Relay.GRPCAddress)

	r.log.Infof("HTTP server listening on '%s'", r.Options.Relay.HTTPListenAddress)

	streams := generateStreams(r.Options.RedisStreams.Streams)

	for {
		streamsResult, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    r.Options.RedisStreams.ConsumerGroup,
			Consumer: r.Options.RedisStreams.ConsumerName,
			Streams:  streams,
			Count:    r.Options.RedisStreams.Count,
			Block:    0,
			NoAck:    false,
		}).Result()

		if err != nil {
			if err == context.Canceled {
				r.log.Info("Received shutdown signal, existing relayer")
				break
			}

			// Temporarily mute stats
			stats.Mute("redis-streams-relay-consumer")
			stats.Mute("redis-streams-relay-producer")

			stats.IncrPromCounter("plumber_read_errors", 1)

			expandedErr := fmt.Errorf("unable to read message(s): %s (retrying in %s)", err, RetryReadInterval)
			util.WriteError(r.log, errorCh, expandedErr)

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
						assertErr := fmt.Errorf("[ID: %s Stream: %s Key: %s] unable to type assert value as string: %s; skipping",
							message.ID, streamName, k, err)
						util.WriteError(r.log, errorCh, assertErr)

						continue
					}

					stats.Incr("redis-streams-relay-consumer", 1)

					// Generate relay message
					relayCh <- &rtypes.RelayMessage{
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

	r.log.Debug("relayer exiting")

	return nil
}

// validateRelayOptions ensures all required CLI options are present before initializing relay mode
func validateRelayOptions(opts *options.Options) error {
	if len(opts.RedisStreams.Streams) == 0 {
		return errMissingStream
	}

	// RedisStreams either supports a password (v1+) OR a username+password (v6+)
	if opts.RedisPubSub.Username != "" && opts.RedisPubSub.Password == "" {
		return errors.New("missing password (either use only password or fill out both)")
	}

	return nil
}
