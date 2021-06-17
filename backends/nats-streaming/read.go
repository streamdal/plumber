package nats_streaming

import (
	"fmt"
	"time"

	pb2 "github.com/nats-io/stan.go/pb"

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
	errInvalidReadOption = errors.New("You may only specify one read option of --last, --all, --seq, --since")
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

	count := 1

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

		str = fmt.Sprintf("%d: ", count) + str
		count++

		n.printer.Print(str)

		// All read options except --last-received will default to follow mode, otherwise we will cause a panic here
		if !n.Options.ReadFollow && n.Options.NatsStreaming.ReadLastReceived {
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

// getReadOptions returns slice of options to pass to Stan.io. Only one read option of --last, --since, --seq, --new--only
// is allowed, so return early once we have a read option
func (n *NatsStreaming) getReadOptions() []stan.SubscriptionOption {
	opts := make([]stan.SubscriptionOption, 0)

	if n.Options.NatsStreaming.DurableSubscription != "" {
		opts = append(opts, stan.DurableName(n.Options.NatsStreaming.DurableSubscription))
	}

	if n.Options.NatsStreaming.AllAvailable {
		opts = append(opts, stan.DeliverAllAvailable())
		return opts
	}

	if n.Options.NatsStreaming.ReadLastReceived {
		opts = append(opts, stan.StartWithLastReceived())
		return opts
	}

	if n.Options.NatsStreaming.ReadSince > 0 {
		opts = append(opts, stan.StartAtTimeDelta(n.Options.NatsStreaming.ReadSince))
		return opts
	}

	if n.Options.NatsStreaming.ReadFromSequence > 0 {
		opts = append(opts, stan.StartAtSequence(n.Options.NatsStreaming.ReadFromSequence))
		return opts
	}

	// Default option is new-only
	opts = append(opts, stan.StartAt(pb2.StartPosition_NewOnly))

	return opts
}

// validateReadOptions ensures the correct CLI options are specified for the read action
func validateReadOptions(opts *cli.Options) error {
	if opts.NatsStreaming.Channel == "" {
		return errMissingChannel
	}

	if opts.NatsStreaming.ReadFromSequence > 0 {
		if opts.NatsStreaming.AllAvailable {
			return errInvalidReadOption
		}

		if opts.NatsStreaming.ReadSince > 0 {
			return errInvalidReadOption
		}

		if opts.NatsStreaming.ReadLastReceived {
			return errInvalidReadOption
		}
	}

	if opts.NatsStreaming.AllAvailable {
		if opts.NatsStreaming.ReadFromSequence > 0 {
			return errInvalidReadOption
		}

		if opts.NatsStreaming.ReadSince > 0 {
			return errInvalidReadOption
		}

		if opts.NatsStreaming.ReadLastReceived {
			return errInvalidReadOption
		}
	}

	if opts.NatsStreaming.ReadSince > 0 {
		if opts.NatsStreaming.ReadFromSequence > 0 {
			return errInvalidReadOption
		}

		if opts.NatsStreaming.AllAvailable {
			return errInvalidReadOption
		}

		if opts.NatsStreaming.ReadLastReceived {
			return errInvalidReadOption
		}
	}

	if opts.NatsStreaming.ReadLastReceived {
		if opts.NatsStreaming.ReadFromSequence > 0 {
			return errInvalidReadOption
		}

		if opts.NatsStreaming.AllAvailable {
			return errInvalidReadOption
		}

		if opts.NatsStreaming.ReadSince > 0 {
			return errInvalidReadOption
		}
	}

	return nil
}
