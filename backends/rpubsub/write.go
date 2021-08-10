package rpubsub

import (
	"context"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/writer"
)

// Write is the entry point function for performing write operations in RedisPubSub.
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

	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to complete initial connect")
	}

	r := &Redis{
		Options: opts,
		client:  client,
		msgDesc: md,
		log:     logrus.WithField("pkg", "redis/write.go"),
	}

	defer client.Close()

	for _, value := range writeValues {
		if err := r.Write(value); err != nil {
			r.log.Error(err)
		}
	}

	return nil
}

func (r *Redis) Write(value []byte) error {
	for _, ch := range r.Options.RedisPubSub.Channels {
		err := r.client.Publish(context.Background(), ch, value).Err()
		if err != nil {
			r.log.Errorf("Failed to publish message to channel '%s': %s", ch, err)
			continue
		}

		r.log.Infof("Successfully wrote message to '%s'", ch)
	}

	return nil
}

func validateWriteOptions(opts *options.Options) error {
	return nil
}
