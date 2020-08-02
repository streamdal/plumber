package rabbitmq

import (
	"encoding/base64"
	"fmt"
	"os"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/util"
)

// Read is the entry point function for performing read operations in RabbitMQ.
//
// This is where we verify that the provided arguments and flag combination
// makes sense/are valid; this is also where we will perform our initial conn.
func Read(c *cli.Context) error {
	opts, err := parseOptions(c)
	if err != nil {
		return errors.Wrap(err, "unable to parse options")
	}

	if err := validateReadOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	var mdErr error
	var md *desc.MessageDescriptor

	if opts.OutputType == "protobuf" {
		md, mdErr = pb.FindMessageDescriptor(opts.ProtobufDir, opts.ProtobufRootMessage)
		if mdErr != nil {
			return errors.Wrap(mdErr, "unable to find root message descriptor")
		}
	}

	ch, err := connect(opts)
	if err != nil {
		return errors.Wrap(err, "unable to complete initial connect")
	}

	r := &RabbitMQ{
		Options: opts,
		Channel: ch,
		MsgDesc: md,
		log:     logrus.WithField("pkg", "rabbitmq/read.go"),
	}

	return r.Read()
}

// Read will attempt to consume one or more messages from the established rabbit
// channel.
//
// NOTE: This method will not tolerate network hiccups. If you plan on running
// this long-term - we should add reconnect support.
func (r *RabbitMQ) Read() error {
	r.log.Info("Listening for message(s) ...")

	msgChan, err := r.Channel.Consume(r.Options.QueueName, "", true, true, false, false, nil)
	if err != nil {
		return errors.Wrap(err, "unable to create initial consume channel")
	}

	lineNumber := 1

	for {
		msg := <-msgChan

		if r.Options.OutputType == "protobuf" {
			decoded, err := pb.DecodeProtobufToJSON(dynamic.NewMessage(r.MsgDesc), msg.Body)
			if err != nil {
				if !r.Options.Follow {
					return fmt.Errorf("unable to decode protobuf message: %s", err)
				}

				printer.Error(fmt.Sprintf("unable to decode protobuf message: %s", err))
				continue
			}

			msg.Body = decoded
		}

		var data []byte
		var convertErr error

		switch r.Options.Convert {
		case "base64":
			_, convertErr = base64.StdEncoding.Decode(data, msg.Body)
		case "gzip":
			data, convertErr = util.Gunzip(msg.Body)
		default:
			data = msg.Body
		}

		if convertErr != nil {
			if !r.Options.Follow {
				return errors.Wrap(convertErr, "unable to complete conversion")
			}

			printer.Error(fmt.Sprintf("unable to complete conversion for message: %s", convertErr))
			continue
		}

		str := string(data)

		if r.Options.LineNumbers {
			str = fmt.Sprintf("%d: ", lineNumber) + str
			lineNumber++
		}

		printer.Print(str)

		if !r.Options.Follow {
			break
		}
	}

	r.log.Debug("reader exiting")

	return nil
}

func validateReadOptions(opts *Options) error {
	if opts.Address == "" {
		return errors.New("--address cannot be empty")
	}

	if opts.Action == "write" && opts.RoutingKey == "" {
		return errors.New("--routing-key cannot be empty with write action")
	}

	if opts.OutputType == "protobuf" {
		if opts.ProtobufDir == "" {
			return errors.New("'--protobuf-dir' must be set when type " +
				"is set to 'protobuf'")
		}

		if opts.ProtobufRootMessage == "" {
			return errors.New("'--protobuf-root-message' must be when " +
				"type is set to 'protobuf'")
		}

		// Does given dir exist?
		if _, err := os.Stat(opts.ProtobufDir); os.IsNotExist(err) {
			return fmt.Errorf("--protobuf-dir '%s' does not exist", opts.ProtobufDir)
		}
	}

	return nil
}
