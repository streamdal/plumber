package gcppubsub

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"sync"

	"cloud.google.com/go/pubsub"
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

	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	r := &GCPPubSub{
		Options: opts,
		MsgDesc: md,
		Client:  client,
		log:     logrus.WithField("pkg", "gcp-pubsub/read.go"),
	}

	return r.Read()
}

func (g *GCPPubSub) Read() error {
	g.log.Info("Listening for message(s) ...")

	sub := g.Client.Subscription(g.Options.GCPPubSub.ReadSubscriptionId)

	// Receive launches several goroutines to exec func, need to use a mutex
	var m sync.Mutex

	lineNumber := 1

	// Standard way to cancel Receive in gcp's pubsub
	cctx, cancel := context.WithCancel(context.Background())

	if err := sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		m.Lock()
		defer m.Unlock()

		if g.Options.GCPPubSub.ReadAck {
			defer msg.Ack()
		}

		if g.Options.ReadOutputType == "protobuf" {
			decoded, err := pb.DecodeProtobufToJSON(dynamic.NewMessage(g.MsgDesc), msg.Data)
			if err != nil {
				if !g.Options.ReadFollow {
					printer.Error(fmt.Sprintf("unable to decode protobuf message: %s", err))
					cancel()
					return
				}

				// Continue running
				printer.Error(fmt.Sprintf("unable to decode protobuf message: %s", err))
				return
			}

			msg.Data = decoded
		}

		// Handle AVRO
		if g.Options.AvroSchemaFile != "" {
			decoded, err := serializers.AvroDecode(g.Options.AvroSchemaFile, msg.Data)
			if err != nil {
				printer.Error(fmt.Sprintf("unable to decode AVRO message: %s", err))
				return
			}
			msg.Data = decoded
		}

		var data []byte
		var convertErr error

		switch g.Options.ReadConvert {
		case "base64":
			_, convertErr = base64.StdEncoding.Decode(data, msg.Data)
		case "gzip":
			data, convertErr = util.Gunzip(msg.Data)
		default:
			data = msg.Data
		}

		if convertErr != nil {
			if !g.Options.ReadFollow {
				printer.Error(fmt.Sprintf("unable to complete conversion for message: %s", convertErr))
				cancel()
				return
			}

			// Continue running
			printer.Error(fmt.Sprintf("unable to complete conversion for message: %s", convertErr))
			return
		}

		str := string(data)

		if g.Options.ReadLineNumbers {
			str = fmt.Sprintf("%d: ", lineNumber) + str
			lineNumber++
		}

		printer.Print(str)

		if !g.Options.ReadFollow {
			cancel()
			return
		}
	}); err != nil {
		return errors.Wrap(err, "unable to complete msg receive")
	}

	g.log.Debug("Reader exiting")

	return nil
}

func validateReadOptions(opts *cli.Options) error {
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		return errors.New("GOOGLE_APPLICATION_CREDENTIALS must be set")
	}

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
