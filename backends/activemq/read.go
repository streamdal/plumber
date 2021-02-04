package activemq

import (
	"fmt"

	"github.com/go-stomp/stomp"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
)

func Read(opts *cli.Options) error {
	if err := validateReadOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	var mdErr error
	var md *desc.MessageDescriptor

	if opts.ReadProtobufRootMessage != "" {
		md, mdErr = pb.FindMessageDescriptor(opts.ReadProtobufDirs, opts.ReadProtobufRootMessage)
		if mdErr != nil {
			return errors.Wrap(mdErr, "unable to find root message descriptor")
		}
	}

	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	r := &ActiveMq{
		Options: opts,
		MsgDesc: md,
		Client:  client,
		log:     logrus.WithField("pkg", "activemq/read.go"),
	}

	return r.Read()
}

func (a *ActiveMq) Read() error {
	a.log.Info("Listening for message(s) ...")

	lineNumber := 1

	sub, _ := a.Client.Subscribe(a.getDestination(), stomp.AckClient)

	for msg := range sub.C {
		data, err := reader.Decode(a.Options, a.MsgDesc, msg.Body)
		if err != nil {
			return err
		}

		str := string(data)

		if a.Options.ReadLineNumbers {
			str = fmt.Sprintf("%d: ", lineNumber) + str
			lineNumber++
		}

		printer.Print(str)

		a.Client.Ack(msg)

		if !a.Options.ReadFollow {
			if err := sub.Unsubscribe(); err != nil {
				return errors.Wrap(err, "unable to unsubcribe from activemq channel")
			}

			if err := a.Client.Disconnect(); err != nil {
				return errors.Wrap(err, "unable to disconnect nicely from activemq server")
			}
			return nil
		}
	}

	a.log.Debug("Reader exiting")
	return nil
}

func validateReadOptions(opts *cli.Options) error {
	if opts.ActiveMq.Topic != "" && opts.ActiveMq.Queue != "" {
		return errors.New("you may only specify a \"topic\" or a \"queue\" not both")
	}

	// If anything protobuf-related is specified, it's being used
	if opts.ReadProtobufRootMessage != "" || len(opts.ReadProtobufDirs) != 0 {
		if err := cli.ValidateProtobufOptions(
			opts.ReadProtobufDirs,
			opts.ReadProtobufRootMessage,
		); err != nil {
			return fmt.Errorf("unable to validate protobuf option(s): %s", err)
		}
	}

	return nil
}
