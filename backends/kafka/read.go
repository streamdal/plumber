package kafka

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"os"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/util"
)

type IReader interface {
	Read(ctx context.Context) (skafka.Message, error)
}

type Reader struct {
	Id                string
	Reader            *skafka.Reader
	Options           *Options
	MessageDescriptor *desc.MessageDescriptor
	log               *logrus.Entry
}

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

	dialer := &skafka.Dialer{
		Timeout: opts.ConnectTimeout,
	}

	if opts.UseInsecureTLS {
		dialer.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	// The dialer timeout does not get utilized under some conditions (such as
	// when kafka is configured to NOT auto create topics) - we need a
	// mechanism to bail out early.
	ctxDeadline, _ := context.WithDeadline(context.Background(), time.Now().Add(opts.ConnectTimeout))

	// Attempt to establish connection on startup
	if _, err := dialer.DialLeader(ctxDeadline, "tcp", opts.Address, opts.Topic, 0); err != nil {
		return fmt.Errorf("unable to create initial connection to host '%s': %s",
			opts.Address, err)
	}

	k := &Kafka{
		Options: opts,
		Dialer:  dialer,
	}

	return k.NewReader("plumber-reader", md, opts).Read(opts.Context)
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

func (k *Kafka) NewReader(id string, md *desc.MessageDescriptor, opts *Options) *Reader {
	readerConfig := skafka.ReaderConfig{
		Brokers:       []string{opts.Address},
		GroupID:       opts.GroupId,
		Topic:         opts.Topic,
		Dialer:        k.Dialer,
		MaxWait:       DefaultMaxWait,
		MaxBytes:      DefaultMaxBytes,
		QueueCapacity: 1,
	}

	return &Reader{
		Id:                id,
		Options:           opts,
		Reader:            skafka.NewReader(readerConfig),
		MessageDescriptor: md,
		log:               logrus.WithField("readerID", id),
	}
}

func (r *Reader) Read(ctx context.Context) error {
	r.log.Info("Initializing (could take a minute or two)")

	lineNumber := 1

	for {
		// Initial message read can take a while to occur due to how consumer
		// groups are setup on initial connect.
		msg, err := r.Reader.ReadMessage(ctx)
		if err != nil {
			if !r.Options.Follow {
				return errors.Wrap(err, "unable to read message")
			}

			printer.Error(fmt.Sprintf("Unable to read message: %s", err))
			continue
		}

		if r.Options.OutputType == "protobuf" {
			decoded, err := pb.DecodeProtobufToJSON(dynamic.NewMessage(r.MessageDescriptor), msg.Value)
			if err != nil {
				if !r.Options.Follow {
					return fmt.Errorf("unable to decode protobuf message: %s", err)
				}

				printer.Error(fmt.Sprintf("unable to decode protobuf message: %s", err))
				continue
			}

			msg.Value = decoded
		}

		var data []byte
		var convertErr error

		switch r.Options.Convert {
		case "base64":
			_, convertErr = base64.StdEncoding.Decode(data, msg.Value)
		case "gzip":
			data, convertErr = util.Gunzip(msg.Value)
		default:
			data = msg.Value
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

	r.log.Debug("Read complete")

	return nil
}
