package pulsar

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber/reader"

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
		printer: printer.New(),
		log:     logrus.WithField("pkg", "pulsar/read.go"),
	}

	return r.Read()
}

func (p *Pulsar) Read() error {
	p.log.Info("Listening for message(s) ...")

	consumer, err := p.Client.Subscribe(pulsar.ConsumerOptions{
		Topic:            p.Options.Pulsar.Topic,
		SubscriptionName: p.Options.Pulsar.SubscriptionName,
		Type:             p.getSubscriptionType(),
	})
	if err != nil {
		return err
	}

	defer consumer.Close()
	defer consumer.Unsubscribe()

	for {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			return err
		}

		data, err := reader.Decode(p.Options, p.MsgDesc, msg.Payload())
		if err != nil {
			return err
		}

		p.printer.Print(string(data))

		consumer.Ack(msg)

		if !p.Options.ReadFollow {
			return nil
		}
	}

	return nil
}

// validateReadOptions ensures all specified read flags are correct
func validateReadOptions(opts *cli.Options) error {
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

// getSubscriptionType converts string input of the subscription type to pulsar library's equivalent
func (p *Pulsar) getSubscriptionType() pulsar.SubscriptionType {
	switch p.Options.Pulsar.SubscriptionType {
	case "exclusive":
		return pulsar.Exclusive
	case "failover":
		return pulsar.Failover
	case "keyshared":
		return pulsar.KeyShared
	default:
		return pulsar.Shared
	}
}
