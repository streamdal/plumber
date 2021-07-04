package rabbitmq

import (
	"context"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/writer"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
)

// Write is the entry point function for performing write operations in RabbitMQ.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func Write(opts *cli.Options, md *desc.MessageDescriptor) error {
	if err := writer.ValidateWriteOptions(opts, nil); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	writeValues, err := writer.GenerateWriteValues(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	r, err := New(opts, md)
	if err != nil {
		return errors.Wrap(err, "unable to initialize rabbitmq consumer")
	}

	defer r.Consumer.Close()

	ctx := context.Background()

	for _, value := range writeValues {
		if err := r.Write(ctx, value); err != nil {
			r.log.Error(err)
		}
	}

	return nil
}

// Write is a wrapper for amqp Publish method. We wrap it so that we can mock
// it in tests, add logging etc.
func (r *RabbitMQ) Write(ctx context.Context, value []byte) error {
	err := r.Consumer.Publish(ctx, r.Options.Rabbit.RoutingKey, value)
	if err != nil {
		return errors.Wrap(err, "unable to write data to rabbit")
	}

	r.log.Infof("Successfully wrote message to exchange '%s'", r.Options.Rabbit.Exchange)
	return nil
}
