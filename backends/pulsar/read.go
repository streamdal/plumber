package pulsar

import (
	"context"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/printer"
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

	r := &Pulsar{
		Options: opts,
		MsgDesc: md,
		Client:  client,
		log:     logrus.WithField("pkg", "pulsar/read.go"),
		printer: printer.New(),
	}

	return r.Read()
}

func (p *Pulsar) Read() error {
	p.log.Info("Listening for message(s) ...")

	reader, err := p.Client.CreateReader(pulsar.ReaderOptions{
		Topic:                  p.Options.Pulsar.Topic,
		StartMessageID:         pulsar.EarliestMessageID(),
		SubscriptionRolePrefix: p.Options.Pulsar.RolePrefix,
	})
	if err != nil {
		return err
	}
	defer reader.Close()

	lineNumber := 0
	for reader.HasNext() {
		msg, err := reader.Next(context.Background())
		if err != nil {
			return err
		}

		str := string(msg.Payload())

		if p.Options.ReadLineNumbers {
			str = fmt.Sprintf("%d: ", lineNumber) + str
			lineNumber++
		}

		p.printer.Print(str)

		if p.Options.ReadFollow {
			return nil
		}
	}

	return nil
}
