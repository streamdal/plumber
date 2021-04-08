package nats_streaming

import (
	"fmt"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/nats-io/stan.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
)

var (
	errMissingChannel    = errors.New("--channel name cannot be empty")
	errInvalidReadOption = errors.New("You may only specify either --from-sequence or --all-available, not both")
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

	n := &NatsStreaming{
		Options: opts,
		MsgDesc: md,
		Client:  client,
		log:     logrus.WithField("pkg", "nats/read.go"),
		printer: printer.New(),
	}

	return n.Read()
}

func (n *NatsStreaming) Read() error {
	defer n.Client.Close()
	n.log.Info("Listening for message(s) ...")

	lineNumber := 1

	// stan.Subscribe is async, use channel to wait to exit
	doneCh := make(chan bool)
	defer close(doneCh)

	subConn, err := stan.Connect(n.Options.NatsStreaming.ClusterID, n.Options.NatsStreaming.ClientID, stan.NatsConn(n.Client))
	if err != nil {
		return errors.Wrap(err, "could not create NATS subscription")
	}

	defer subConn.Close()

	subFunc := func(msg *stan.Msg) {
		data, err := reader.Decode(n.Options, n.MsgDesc, msg.Data)
		if err != nil {
			n.log.Error(err)
			return
		}

		if n.Options.Verbose {
			n.printer.Print("")
			logrus.Infof("- %-24s%-6s", "Timestamp", time.Unix(0, msg.Timestamp).UTC().String())
			logrus.Infof("- %-24s%d", "Sequence No.", msg.Sequence)
			logrus.Infof("- %-24s%-6d", "CRC32", msg.CRC32)
			logrus.Infof("- %-24s%-6t", "Redelivered", msg.Redelivered)
			logrus.Infof("- %-24s%-6d", "Redelivery Count", msg.RedeliveryCount)
			logrus.Infof("- %-24s%-6s", "Subject", msg.Subject)
		}

		str := string(data)

		if n.Options.ReadLineNumbers {
			str = fmt.Sprintf("%d: ", lineNumber) + str
			lineNumber++
		}

		n.printer.Print(str)

		if !n.Options.ReadFollow {
			doneCh <- true
		}
	}

	sub, err := subConn.Subscribe(n.Options.NatsStreaming.Channel, subFunc, n.getReadOptions()...)
	if err != nil {
		return errors.Wrap(err, "unable to create subscription")
	}

	defer sub.Unsubscribe()

	<-doneCh

	return nil
}

func (n *NatsStreaming) getReadOptions() []stan.SubscriptionOption {
	opts := make([]stan.SubscriptionOption, 0)

	if n.Options.NatsStreaming.AllAvailable {
		opts = append(opts, stan.DeliverAllAvailable())
	}

	if n.Options.NatsStreaming.StartReadingFrom > 0 {
		opts = append(opts, stan.StartAtSequence(n.Options.NatsStreaming.StartReadingFrom))
	}

	if n.Options.NatsStreaming.DurableSubscription != "" {
		opts = append(opts, stan.DurableName(n.Options.NatsStreaming.DurableSubscription))
	}

	return opts
}

// validateReadOptions ensures the correct CLI options are specified for the read action
func validateReadOptions(opts *cli.Options) error {
	if opts.NatsStreaming.Channel == "" {
		return errMissingChannel
	}

	if opts.NatsStreaming.StartReadingFrom > 0 && opts.NatsStreaming.AllAvailable {
		return errInvalidReadOption
	}
	return nil
}
