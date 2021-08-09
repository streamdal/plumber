package activemq

import (
	"fmt"

	"github.com/go-stomp/stomp"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
)

func Read(opts *options.Options, md *desc.MessageDescriptor) error {
	if err := validateReadOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
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
	defer a.Client.Disconnect()

	a.log.Info("Listening for message(s) ...")

	count := 1

	sub, err := a.Client.Subscribe(a.getDestination(), stomp.AckClient)
	if err != nil {
		return errors.Wrap(err, "unable to create subscription")
	}

	defer sub.Unsubscribe()

	for msg := range sub.C {
		data, err := reader.Decode(a.Options, a.MsgDesc, msg.Body)
		if err != nil {
			return err
		}

		str := string(data)

		str = fmt.Sprintf("%d: ", count) + str
		count++

		printer.Print(str)

		a.Client.Ack(msg)

		if !a.Options.ReadFollow {
			return nil
		}
	}

	a.log.Debug("Reader exiting")
	return nil
}

func validateReadOptions(opts *options.Options) error {
	if opts.ActiveMq.Topic != "" && opts.ActiveMq.Queue != "" {
		return errors.New("you may only specify a \"topic\" or a \"queue\" not both")
	}

	return nil
}
