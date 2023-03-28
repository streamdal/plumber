package rstreams

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

func (r *RedisStreams) Read(ctx context.Context, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord) error {
	if err := validateReadOptions(readOpts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	if err := r.createConsumerGroups(ctx, readOpts.RedisStreams.Args); err != nil {
		return fmt.Errorf("unable to create consumer group(s): %s", err)
	}

	r.log.Info("Listening for message(s) ...")

	var count int64

	for {
		// Attempt to consume
		streamsResult, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    readOpts.RedisStreams.Args.ConsumerGroup,
			Consumer: readOpts.RedisStreams.Args.ConsumerName,
			Streams:  generateStreams(readOpts.RedisStreams.Args.Streams),
			Count:    int64(readOpts.RedisStreams.Args.Count),
			Block:    0,
			NoAck:    false,
		}).Result()

		if err != nil {
			// Don't report ctrl+c as an error
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("unable to read from streamsResult: %s", err)
		}

		// We may be reading from multiple streamsResult - read each stream resp
		for _, stream := range streamsResult {
			r.handleResult(stream, readOpts, resultsChan, errorChan, count)
		}

		if !readOpts.Continuous {
			return nil
		}
	}

	return nil
}

func (r *RedisStreams) handleResult(stream redis.XStream, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord, count int64) {
	streamName := stream.Stream

	// Each stream result may contain multiple messages
	for _, message := range stream.Messages {
		// A single message may contain multiple kv's
		for k, v := range message.Values {
			stringData, ok := v.(string)
			if !ok {
				util.WriteError(r.log, errorChan, fmt.Errorf("[ID: %s Stream: %s Key: %s] unable to type assert value as string: %s; skipping",
					message.ID, streamName, k, v))

				if !readOpts.Continuous {
					break
				}
				continue
			}

			serializedMsg, err := json.Marshal(stringData)
			if err != nil {
				errorChan <- &records.ErrorRecord{
					OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
					Error:               errors.Wrap(err, "unable to serialize message into JSON").Error(),
				}
				continue
			}

			count++

			resultsChan <- &records.ReadRecord{
				MessageId:           uuid.NewV4().String(),
				Num:                 count,
				Metadata:            nil,
				ReceivedAtUnixTsUtc: time.Now().UTC().Unix(),
				Payload:             []byte(stringData),
				XRaw:                serializedMsg,
				Record: &records.ReadRecord_RedisStreams{
					RedisStreams: &records.RedisStreams{
						Id:        message.ID,
						Value:     stringData,
						Stream:    streamName,
						Timestamp: time.Now().UTC().Unix(),
					},
				},
			}
		}
	}
}

func (r *RedisStreams) createConsumerGroups(ctx context.Context, rsArgs *args.RedisStreamsReadArgs) error {

	var offset string
	switch rsArgs.CreateConsumerConfig.OffsetStart {
	case args.OffsetStart_OLDEST:
		offset = "0"
	case args.OffsetStart_LATEST:
		offset = "$"
	default:
		offset = rsArgs.CreateConsumerConfig.OffsetStart.String()
	}

	for _, stream := range rsArgs.Streams {
		if rsArgs.CreateConsumerConfig != nil && rsArgs.CreateConsumerConfig.RecreateConsumerGroup {
			logrus.Debugf("deleting consumer group '%s'", rsArgs.ConsumerGroup)

			_, err := r.client.XGroupDestroy(ctx, stream, rsArgs.ConsumerGroup).Result()
			if err != nil {
				return fmt.Errorf("unable to recreate consumer group: %s", err)
			}
		}

		logrus.Debugf("Creating stream with start id '%s'", rsArgs.CreateConsumerConfig.OffsetStart.String())

		var err error

		if rsArgs.CreateConsumerConfig.CreateStreams {
			_, err = r.client.XGroupCreateMkStream(ctx, stream, rsArgs.ConsumerGroup, offset).Result()
		} else {
			_, err = r.client.XGroupCreate(ctx, stream, rsArgs.ConsumerGroup, offset).Result()
		}

		if err != nil {
			// No problem if consumer group already exists
			if err.Error() != "BUSYGROUP Consumer Group name already exists" {
				return fmt.Errorf("error creating consumer group for stream '%s': %s", stream, err)
			}
		}
	}

	return nil
}

func generateStreams(streams []string) []string {
	for i := 0; i != len(streams); i++ {
		streams = append(streams, ">")
		i++
	}

	return streams
}

func validateReadOptions(readOpts *opts.ReadOptions) error {
	if readOpts == nil {
		return validate.ErrMissingReadOptions
	}

	if readOpts.RedisStreams == nil {
		return validate.ErrEmptyBackendGroup
	}

	if readOpts.RedisStreams.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if len(readOpts.RedisStreams.Args.Streams) == 0 {
		return ErrMissingStream
	}

	return nil
}
