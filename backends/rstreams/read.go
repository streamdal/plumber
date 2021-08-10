package rstreams

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
)

func Read(opts *options.Options, md *desc.MessageDescriptor) error {
	client, err := NewStreamsClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	r := &RedisStreams{
		Options: opts,
		client:  client,
		msgDesc: md,
		ctx:     context.Background(),
		log:     logrus.WithField("pkg", "rstreams/read.go"),
	}

	// Create consumer group (and stream) for each stream
	if err := CreateConsumerGroups(r.ctx, client, r.Options.RedisStreams); err != nil {
		return fmt.Errorf("unable to create consumer group(s): %s", err)
	}

	return r.Read()
}

func generateStreams(streams []string) []string {
	for i := 0; i != len(streams); i++ {
		streams = append(streams, ">")
		i++
	}

	return streams
}

func (r *RedisStreams) Read() error {
	defer r.client.Close()

	streams := generateStreams(r.Options.RedisStreams.Streams)

	r.log.Info("Listening for message(s) ...")

	count := 1

	for {
		// Attempt to consume
		streamsResult, err := r.client.XReadGroup(r.ctx, &redis.XReadGroupArgs{
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

					decodedData, err := reader.Decode(r.Options, r.msgDesc, []byte(stringData))
					if err != nil {
						r.log.Errorf("[ID: %s Stream: %s Key: %s] unable to decode message: %s; skipping",
							message.ID, streamName, k, err)
						continue
					}

					str := fmt.Sprintf("[ID: %s Stream: %s Key: %s] %s", message.ID, streamName, k, string(decodedData))

					str = fmt.Sprintf("%d: %s", count, str)
					count++

					printer.Print(str)
				}
			}
		}

		if !r.Options.ReadFollow {
			return nil
		}
	}
}
