package nats

import (
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"github.com/nats-io/nats.go"
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

	n := &Nats{
		Options: opts,
		MsgDesc: md,
		Client:  client,
		log:     logrus.WithField("pkg", "nats/read.go"),
	}

	return n.Read()
}

func (n *Nats) Read() error {
	defer n.Client.Close()
	n.log.Info("Listening for message(s) ...")

	count := 1

	// nats.Subscribe is async, use channel to wait to exit
	doneCh := make(chan bool)
	defer close(doneCh)

	n.Client.Subscribe(n.Options.Nats.Subject, func(msg *nats.Msg) {
		data, err := reader.Decode(n.Options, n.MsgDesc, msg.Data)
		if err != nil {
			n.log.Error(err)
			return
		}

		str := string(data)

		str = fmt.Sprintf("%d: ", count) + str
		count++

		printer.Print(str)

		if !n.Options.ReadFollow {
			doneCh <- true
		}
	})

	<-doneCh

	return nil
}

// validateReadOptions ensures the correct CLI options are specified for the read action
func validateReadOptions(opts *cli.Options) error {
	if opts.Nats.Subject == "" {
		return errMissingSubject
	}
	return nil
}
