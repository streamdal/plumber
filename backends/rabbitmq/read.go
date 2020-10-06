package rabbitmq

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/batchcorp/rabbit"
	"github.com/streadway/amqp"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/util"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
)

// Read is the entry point function for performing read operations in RabbitMQ.
//
// This is where we verify that the provided arguments and flag combination
// makes sense/are valid; this is also where we will perform our initial conn.
func Read(opts *cli.Options) error {
	if err := validateReadOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	var mdErr error
	var md *desc.MessageDescriptor

	if opts.Rabbit.ReadOutputType == "protobuf" {
		md, mdErr = pb.FindMessageDescriptor(opts.Rabbit.ReadProtobufDirs, opts.Rabbit.ReadProtobufRootMessage)
		if mdErr != nil {
			return errors.Wrap(mdErr, "unable to find root message descriptor")
		}
	}

	r, err := New(opts, md)

	if err != nil {
		return err
	}

	return r.Read()
}

// Read will attempt to consume one or more messages from the established rabbit
// channel.
func (r *RabbitMQ) Read() error {
	r.log.Info("Listening for message(s) ...")

	errCh := make(chan *rabbit.ConsumeError)
	ctx, cancel := context.WithCancel(context.Background())

	go r.Consumer.Consume(ctx, errCh, func(msg amqp.Delivery) error {

		lineNumber := 1

		if r.Options.Rabbit.ReadOutputType == "protobuf" {
			decoded, err := pb.DecodeProtobufToJSON(dynamic.NewMessage(r.MsgDesc), msg.Body)
			if err != nil {
				if !r.Options.Rabbit.ReadFollow {
					return fmt.Errorf("unable to decode protobuf message: %s", err)
				}

				printer.Error(fmt.Sprintf("unable to decode protobuf message: %s", err))
				return nil
			}

			msg.Body = decoded
		}

		var data []byte
		var convertErr error

		switch r.Options.Rabbit.ReadConvert {
		case "base64":
			_, convertErr = base64.StdEncoding.Decode(data, msg.Body)
		case "gzip":
			data, convertErr = util.Gunzip(msg.Body)
		default:
			data = msg.Body
		}

		if convertErr != nil {
			if !r.Options.Rabbit.ReadFollow {
				return errors.Wrap(convertErr, "unable to complete conversion")
			}

			printer.Error(fmt.Sprintf("unable to complete conversion for message: %s", convertErr))
			return errors.Wrap(convertErr, "unable to complete conversion for message")
		}

		str := string(data)

		if r.Options.Rabbit.ReadLineNumbers {
			str = fmt.Sprintf("%d: ", lineNumber) + str
			lineNumber++
		}

		printer.Print(str)

		if !r.Options.Rabbit.ReadFollow {
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

func validateReadOptions(opts *cli.Options) error {
	if opts.Rabbit.Address == "" {
		return errors.New("--address cannot be empty")
	}

	if opts.Action == "write" && opts.Rabbit.RoutingKey == "" {
		return errors.New("--routing-key cannot be empty with write action")
	}

	if opts.Rabbit.ReadOutputType == "protobuf" {
		if err := cli.ValidateProtobufOptions(
			opts.Rabbit.ReadProtobufDirs,
			opts.Rabbit.ReadProtobufRootMessage,
		); err != nil {
			return fmt.Errorf("unable to validate protobuf option(s): %s", err)
		}
	}

	return nil
}
