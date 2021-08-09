package rabbitmq

import (
	"context"
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
	"github.com/batchcorp/rabbit"
)

// Read is the entry point function for performing read operations in RabbitMQ.
//
// This is where we verify that the provided arguments and flag combination
// makes sense/are valid; this is also where we will perform our initial conn.
func Read(opts *options.Options, md *desc.MessageDescriptor) error {
	if err := validateReadOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	r, err := New(opts, md)

	if err != nil {
		return errors.Wrap(err, "unable to initialize rabbitmq consumer")
	}

	return r.Read()
}

// Read will attempt to consume one or more messages from the established rabbit
// channel.
func (r *RabbitMQ) Read() error {
	defer r.Consumer.Close()

	r.log.Info("Listening for message(s) ...")

	errCh := make(chan *rabbit.ConsumeError)
	ctx, cancel := context.WithCancel(context.Background())

	count := 1

	go r.Consumer.Consume(ctx, errCh, func(msg amqp.Delivery) error {

		data, err := reader.Decode(r.Options, r.MsgDesc, msg.Body)
		if err != nil {
			return err
		}

		str := string(data)

		str = fmt.Sprintf("%d: ", count) + str
		count++

		printer.Print(str)

		if !r.Options.ReadFollow {
			cancel()
		}

		return nil
	})

	for {
		select {
		case err := <-errCh:
			return err.Error
		case <-ctx.Done():
			r.log.Debug("Reader exiting")
			return nil
		}
	}

	return nil
}

func validateReadOptions(opts *options.Options) error {
	if opts.Rabbit.Address == "" {
		return errors.New("--address cannot be empty")
	}

	return nil
}
