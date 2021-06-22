package rpubsub

import (
	"context"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/writer"
)

// Write is the entry point function for performing write operations in RedisPubSub.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func Write(opts *cli.Options) error {
	if err := writer.ValidateWriteOptions(opts, validateWriteOptions); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	var mdErr error
	var md *desc.MessageDescriptor

	if opts.WriteInputType == "jsonpb" {
		md, mdErr = pb.FindMessageDescriptor(opts.WriteProtobufDirs, opts.WriteProtobufRootMessage)
		if mdErr != nil {
			return errors.Wrap(mdErr, "unable to find root message descriptor")
		}
	}

	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to complete initial connect")
	}

	r := &Redis{
		Options: opts,
		Client:  client,
		MsgDesc: md,
		log:     logrus.WithField("pkg", "redis/write.go"),
	}

	defer client.Close()

	msg, err := writer.GenerateWriteValue(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	return r.Write(msg)
}

func (r *Redis) Write(value []byte) error {
	for _, ch := range r.Options.RedisPubSub.Channels {
		err := r.Client.Publish(context.Background(), ch, value).Err()
		if err != nil {
			r.log.Errorf("Failed to publish message to channel '%s': %s", ch, err)
			continue
		}

		r.log.Infof("Successfully wrote message to '%s'", ch)
	}

	return nil
}

func validateWriteOptions(opts *cli.Options) error {
	return nil
}
