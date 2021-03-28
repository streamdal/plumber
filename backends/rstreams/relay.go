package rstreams

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/backends/rstreams/types"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/reader"
	"github.com/batchcorp/plumber/relay"
)

const (
	RetryReadInterval = 5 * time.Second
)

type Relayer struct {
	Client  *redis.Client
	Options *cli.Options
	MsgDesc *desc.MessageDescriptor
	RelayCh chan interface{}
	log     *logrus.Entry
	Looper  *director.FreeLooper
	Context context.Context
}

type IRedisStreamsRelayer interface {
	Relay() error
}

var (
	errMissingStream = errors.New("You must specify at least one stream")
)

// Relay sets up a new RedisStreams relayer, starts GRPC workers and the API server
func Relay(opts *cli.Options) error {
	if err := validateRelayOptions(opts); err != nil {
		return errors.Wrap(err, "unable to verify options")
	}

	// Create new relayer instance (+ validate token & gRPC address)
	relayCfg := &relay.Config{
		Token:       opts.RelayToken,
		GRPCAddress: opts.RelayGRPCAddress,
		NumWorkers:  opts.RelayNumWorkers,
		Timeout:     opts.RelayGRPCTimeout,
		RelayCh:     make(chan interface{}, 1),
		DisableTLS:  opts.RelayGRPCDisableTLS,
	}

	grpcRelayer, err := relay.New(relayCfg)
	if err != nil {
		return errors.Wrap(err, "unable to create new gRPC relayer")
	}

	// Launch HTTP server
	go func() {
		if err := api.Start(opts.RelayHTTPListenAddress, opts.Version); err != nil {
			logrus.Fatalf("unable to start API server: %s", err)
		}
	}()

	if err := grpcRelayer.StartWorkers(); err != nil {
		return errors.Wrap(err, "unable to start gRPC relay workers")
	}

	client, err := NewStreamsClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	r := &Relayer{
		Client:  client,
		Options: opts,
		RelayCh: relayCfg.RelayCh,
		log:     logrus.WithField("pkg", "rstreams/relay"),
		Looper:  director.NewFreeLooper(director.FOREVER, make(chan error)),
		Context: context.Background(),
	}

	// Create consumer group (and stream) for each stream
	if err := CreateConsumerGroups(r.Context, client, r.Options.RedisStreams); err != nil {
		return fmt.Errorf("unable to create consumer group(s): %s", err)
	}

	return r.Relay()
}

// validateRelayOptions ensures all required CLI options are present before initializing relay mode
func validateRelayOptions(opts *cli.Options) error {
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
		streamsResult, err := r.Client.XReadGroup(r.Context, &redis.XReadGroupArgs{
			Group:    r.Options.RedisStreams.ConsumerGroup,
			Consumer: r.Options.RedisStreams.ConsumerName,
			Streams:  streams,
			Count:    r.Options.RedisStreams.Count,
			Block:    0,
			NoAck:    false,
		}).Result()

		if err != nil {
			return fmt.Errorf("unable to read from streamsResult: %s", err)
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

					decodedData, err := reader.Decode(r.Options, r.MsgDesc, []byte(stringData))
					if err != nil {
						r.log.Errorf("[ID: %s Stream: %s Key: %s] unable to decode message: %s; skipping",
							message.ID, streamName, k, err)
						continue
					}

					// Generate relay message
					r.RelayCh <- &types.RelayMessage{
						ID:     message.ID,
						Key:    k,
						Stream: streamName,
						Value:  decodedData,
					}

					r.log.Debugf("[ID: %s Stream: %s Key: %s] successfully relayed message", message.ID, streamName, k)
				}
			}
		}
	}
}
