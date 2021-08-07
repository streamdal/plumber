package kubemq_queue

import (
	"context"
	"fmt"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
	"github.com/jhump/protoreflect/desc"
	"github.com/kubemq-io/kubemq-go/queues_stream"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func Read(opts *cli.Options, md *desc.MessageDescriptor) error {
	if err := validateReadOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	k := &KubeMQQueue{
		Options: opts,
		MsgDesc: md,
		Client:  client,
		log:     logrus.WithField("pkg", "kubemq-queue/read.go"),
	}

	return k.Read()
}

func (k *KubeMQQueue) Read() error {
	defer func() {
		_ = k.Client.Close()
	}()
	k.log.Info("Listening for message(s) ...")
	count := 1
	for {
		response, err := k.Client.Poll(context.Background(),
			queues_stream.NewPollRequest().
				SetChannel(k.Options.KubeMQQueue.Queue).
				SetMaxItems(1).
				SetAutoAck(false).
				SetWaitTimeout(10000))
		if err != nil {
			return err
		}
		if response.HasMessages() {
			data, err := reader.Decode(k.Options, k.MsgDesc, response.Messages[0].Body)
			if err != nil {
				return err
			}
			if err := response.AckAll(); err != nil {
				return err
			}
			str := string(data)

			str = fmt.Sprintf("%d: ", count) + str
			count++

			printer.Print(str)

			if !k.Options.ReadFollow {
				return nil
			}
		}
	}
}

// validateReadOptions ensures the correct CLI options are specified for the read action
func validateReadOptions(opts *cli.Options) error {
	if opts.KubeMQQueue.Queue == "" {
		return errMissingQueue
	}
	return nil
}
