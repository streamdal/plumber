package rstreams

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/writer"
)

// Write is the entry point function for performing write operations in RedisStreams.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func Write(opts *cli.Options) error {
	if err := writer.ValidateWriteOptions(opts, validateWriteOptions); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	var mdErr error
	var md *desc.MessageDescriptor

	if opts.WriteInputType == "jsonpb" {
		md, mdErr = pb.FindMessageDescriptor(opts.WriteProtobufDirs, opts.WriteProtobufRootMessage)
		if mdErr != nil {
			return errors.Wrap(mdErr, "unable to find root message descriptor")
		}
	}

	client, err := NewStreamsClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	r := &RedisStreams{
		Options: opts,
		Client:  client,
		MsgDesc: md,
		Context: context.Background(),
		log:     logrus.WithField("pkg", "rstreams/write.go"),
	}

	defer client.Close()

	msg, err := writer.GenerateWriteValue(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	return r.Write(msg)
}

func (r *RedisStreams) Write(value []byte) error {
	for _, streamName := range r.Options.RedisStreams.Streams {
		_, err := r.Client.XAdd(r.Context, &redis.XAddArgs{
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

		r.log.Infof("successfully wrote message to stream '%s' with key '%s'",
			streamName, r.Options.RedisStreams.WriteKey)
	}

	return nil
}

func validateWriteOptions(opts *cli.Options) error {
	return nil
}
