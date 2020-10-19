package kafka

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/serializers"
	"github.com/batchcorp/plumber/util"
)

// Read is the entry point function for performing read operations in Kafka.
//
// This is where we verify that the provided arguments and flag combination
// makes sense/are valid; this is also where we will perform our initial conn.
func Read(opts *cli.Options) error {
	if err := validateReadOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	var mdErr error
	var md *desc.MessageDescriptor

	if opts.ReadOutputType == "protobuf" {
		md, mdErr = pb.FindMessageDescriptor(opts.ReadProtobufDirs, opts.ReadProtobufRootMessage)
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
		msg, err := k.Reader.ReadMessage(context.Background())
		if err != nil {
			if !k.Options.ReadFollow {
				return errors.Wrap(err, "unable to read message")
			}

			printer.Error(fmt.Sprintf("Unable to read message: %s", err))
			continue
		}

		if k.Options.ReadOutputType == "protobuf" {
			decoded, err := pb.DecodeProtobufToJSON(dynamic.NewMessage(k.MessageDesc), msg.Value)
			if err != nil {
				if !k.Options.ReadFollow {
					return fmt.Errorf("unable to decode protobuf message: %s", err)
				}

				printer.Error(fmt.Sprintf("unable to decode protobuf message: %s", err))
				continue
			}

			msg.Value = decoded
		}

		// Handle AVRO
		if k.Options.AvroSchemaFile != "" {
			decoded, err := serializers.AvroDecode(k.Options.AvroSchemaFile, msg.Value)
			if err != nil {
				printer.Error(fmt.Sprintf("unable to decode AVRO message: %s", err))
				return err
			}
			msg.Value = decoded
		}

		data := make([]byte, 0)

		var convertErr error

		switch k.Options.ReadConvert {
		case "base64":
			data, convertErr = base64.StdEncoding.DecodeString(string(msg.Value))
		case "gzip":
			data, convertErr = util.Gunzip(msg.Value)
		default:
			data = msg.Value
		}

		if convertErr != nil {
			if !k.Options.ReadFollow {
				return errors.Wrap(convertErr, "unable to complete conversion")
			}

			printer.Error(fmt.Sprintf("unable to complete conversion for message: %s", convertErr))
			continue
		}

		str := string(data)

		if k.Options.ReadLineNumbers {
			str = fmt.Sprintf("%d: ", lineNumber) + str
			lineNumber++
		}

		printer.Print(str)

		if !k.Options.ReadFollow {
			break
		}
	}

	k.log.Debug("Reader exiting")

	return nil
}

func validateReadOptions(opts *cli.Options) error {
	// If type is protobuf, ensure both --protobuf-dir and --protobuf-root-message
	// are set as well
	if opts.ReadOutputType == "protobuf" {
		if err := cli.ValidateProtobufOptions(
			opts.ReadProtobufDirs,
			opts.ReadProtobufRootMessage,
		); err != nil {
			return fmt.Errorf("unable to validate protobuf option(s): %s", err)
		}
	}

	return nil
}
