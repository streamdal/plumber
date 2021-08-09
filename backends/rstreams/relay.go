package rstreams

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/backends/rstreams/types"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/relay"
	"github.com/batchcorp/plumber/stats"
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

// Relay sets up a new RedisStreams relayer
func Relay(opts *options.Options, relayCh chan interface{}, shutdownCtx context.Context) (relay.IRelayBackend, error) {
	if err := validateRelayOptions(opts); err != nil {
		return nil, errors.Wrap(err, "unable to verify options")
	}

	client, err := NewStreamsClient(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create client")
	}

	r := &Relayer{
		Client:      client,
		Options:     opts,
		RelayCh:     relayCh,
		log:         logrus.WithField("pkg", "rstreams/relay"),
		Looper:      director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
		ShutdownCtx: shutdownCtx,
	}

	// Create consumer group (and stream) for each stream
	if err := CreateConsumerGroups(r.ShutdownCtx, client, r.Options.RedisStreams); err != nil {
		return nil, fmt.Errorf("unable to create consumer group(s): %s", err)
	}

	return r, nil
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

// Relay reads messages from RedisStreams and sends them to RelayCh which is then read by relay.Run()
func (r *Relayer) Relay() error {
	r.log.Infof("Relaying RedisStreams messages from %d stream(s) (%s) -> '%s'",
		len(r.Options.RedisStreams.Streams), r.Options.RedisStreams.Streams, r.Options.RelayGRPCAddress)

	r.log.Infof("HTTP server listening on '%s'", r.Options.RelayHTTPListenAddress)

	defer r.Client.Close()

	streams := generateStreams(r.Options.RedisStreams.Streams)

	for {
		streamsResult, err := r.Client.XReadGroup(r.ShutdownCtx, &redis.XReadGroupArgs{
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
				return nil
			}

			// Temporarily mute stats
			stats.Mute("redis-streams-relay-consumer")
			stats.Mute("redis-streams-relay-producer")

			stats.IncrPromCounter("plumber_read_errors", 1)

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
						r.log.Errorf("[ID: %s Stream: %s Key: %s] unable to type assert value as string: %s; skipping",
							message.ID, streamName, k, err)

						continue
					}

					stats.Incr("redis-streams-relay-consumer", 1)

					// Generate relay message
					r.RelayCh <- &types.RelayMessage{
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
