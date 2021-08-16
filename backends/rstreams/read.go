package rstreams

import (
	"context"
	"fmt"
	"time"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/go-redis/redis/v8"
)

func (r *RedisStreams) Read(ctx context.Context, resultsChan chan *types.ReadMessage, errorChan chan *types.ErrorMessage) error {
	// Create consumer group (and stream) for each stream
	if err := createConsumerGroups(ctx, r.client, r.Options.RedisStreams); err != nil {
		return fmt.Errorf("unable to create consumer group(s): %s", err)
	}

	streams := generateStreams(r.Options.RedisStreams.Streams)

	r.log.Info("Listening for message(s) ...")

	count := 1

	for {
		// Attempt to consume
		streamsResult, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
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
						assertErr := fmt.Errorf("[ID: %s Stream: %s Key: %s] unable to type assert value as string: %s; skipping",
							message.ID, streamName, k, err)
						util.WriteError(r.log, errorChan, assertErr)

						continue
					}

					//str := fmt.Sprintf("[ID: %s Stream: %s Key: %s] %s", message.ID, streamName, k, string(decodedData))
					resultsChan <- &types.ReadMessage{
						Value: []byte(stringData),
						Metadata: map[string]interface{}{
							"key": k,
						},
						ReceivedAt: time.Now().UTC(),
						Num:        count,
						Raw:        message,
					}

					count++
				}
			}
		}

		if !r.Options.Read.Follow {
			break
		}
	}

	r.log.Debug("read exiting")

	return nil
}

func generateStreams(streams []string) []string {
	for i := 0; i != len(streams); i++ {
		streams = append(streams, ">")
		i++
	}

	return streams
}
