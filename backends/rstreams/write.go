package rstreams

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/writer"
)

// Write is the entry point function for performing write operations in RedisStreams.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func Write(opts *options.Options, md *desc.MessageDescriptor) error {
	if err := writer.ValidateWriteOptions(opts, validateWriteOptions); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	writeValues, err := writer.GenerateWriteValues(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	client, err := NewStreamsClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	r := &RedisStreams{
		Options: opts,
		client:  client,
		msgDesc: md,
		ctx:     context.Background(),
		log:     logrus.WithField("pkg", "rstreams/write.go"),
	}

	defer client.Close()

	for _, value := range writeValues {
		if err := r.Write(value); err != nil {
			r.log.Error(err)
		}
	}

	return nil
}

func (r *RedisStreams) Write(value []byte) error {
	for _, streamName := range r.Options.RedisStreams.Streams {
		_, err := r.client.XAdd(r.ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     r.Options.RedisStreams.WriteID,
			Values: map[string]interface{}{
				r.Options.RedisStreams.WriteKey: value,
			},
		}).Result()

		if err != nil {
			r.log.Errorf("unable to write message to stream '%s': %s", streamName, err)
			continue
		}

		r.log.Infof("Successfully wrote message to stream '%s' with key '%s'",
			streamName, r.Options.RedisStreams.WriteKey)
	}

	return nil
}

func validateWriteOptions(opts *options.Options) error {
	return nil
}
