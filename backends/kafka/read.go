package kafka

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

// Read is the entry point function for performing read operations in Kafka.
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

	reader, err := NewReader(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create new reader")
	}

	k := &Kafka{
		Options:     opts,
		MessageDesc: md,
		Reader:      reader,
		log:         logrus.WithField("pkg", "kafka/read.go"),
	}

	return k.Read()
}

// Read will attempt to consume one or more messages from a given topic,
// optionally decode it and/or convert the returned output.
//
// This method SHOULD be able to recover from network hiccups.
func (k *Kafka) Read() error {
	k.log.Info("Initializing (could take a minute or two) ...")

	lineNumber := 1

	for {
		// Initial message read can take a while to occur due to how consumer
		// groups are setup on initial connect.
		msg, err := k.Reader.ReadMessage(k.Options.Context)
		if err != nil {
			if !k.Options.Follow {
				return errors.Wrap(err, "unable to read message")
			}

			printer.Error(fmt.Sprintf("Unable to read message: %s", err))
			continue
		}

		if k.Options.OutputType == "protobuf" {
			decoded, err := pb.DecodeProtobufToJSON(dynamic.NewMessage(k.MessageDesc), msg.Value)
			if err != nil {
				if !k.Options.Follow {
					return fmt.Errorf("unable to decode protobuf message: %s", err)
				}

				printer.Error(fmt.Sprintf("unable to decode protobuf message: %s", err))
				continue
			}

			msg.Value = decoded
		}

		var data []byte
		var convertErr error

		switch k.Options.Convert {
		case "base64":
			_, convertErr = base64.StdEncoding.Decode(data, msg.Value)
		case "gzip":
			data, convertErr = util.Gunzip(msg.Value)
		default:
			data = msg.Value
		}

		if convertErr != nil {
			if !k.Options.Follow {
				return errors.Wrap(convertErr, "unable to complete conversion")
			}

			printer.Error(fmt.Sprintf("unable to complete conversion for message: %s", convertErr))
			continue
		}

		str := string(data)

		if k.Options.LineNumbers {
			str = fmt.Sprintf("%d: ", lineNumber) + str
			lineNumber++
		}

		printer.Print(str)

		if !k.Options.Follow {
			break
		}
	}

	k.log.Debug("Reader exiting")

	return nil
}

func validateReadOptions(opts *Options) error {
	// If type is protobuf, ensure both --protobuf-dir and --protobuf-root-message
	// are set as well
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
