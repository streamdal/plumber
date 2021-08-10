package azure

import (
	"context"
	"fmt"

	servicebus "github.com/Azure/azure-service-bus-go"
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

	a := &ServiceBus{
		Options: opts,
		msgDesc: md,
		client:  client,
		log:     logrus.WithField("pkg", "azure/read.go"),
	}

	if opts.Azure.Queue != "" {
		queue, err := client.NewQueue(opts.Azure.Queue)
		if err != nil {
			return errors.Wrap(err, "unable to create new azure service bus queue client")
		}

		a.queue = queue
	} else {
		topic, err := client.NewTopic(opts.Azure.Topic)
		if err != nil {
			return errors.Wrap(err, "unable to create new azure service bus topic client")
		}

		a.topic = topic
	}

	return a.Read()
}

func (a *ServiceBus) Read() error {
	ctx := context.Background()

	a.log.Info("Listening for message(s) ...")

	count := 1

	var handler servicebus.HandlerFunc = func(ctx context.Context, msg *servicebus.Message) error {
		data, err := reader.Decode(a.Options, a.msgDesc, msg.Data)
		if err != nil {
			return err
		}

		str := string(data)

		str = fmt.Sprintf("%d: ", count) + str
		count++

		printer.Print(str)

		return msg.Complete(ctx)
	}

	if a.queue != nil {
		return a.readQueue(ctx, handler)
	}

	if a.topic != nil {
		return a.readTopic(ctx, handler)
	}

	return nil
}

// readQueue reads messages from an ASB queue
func (a *ServiceBus) readQueue(ctx context.Context, handler servicebus.HandlerFunc) error {
	defer a.queue.Close(ctx)
	for {
		if err := a.queue.ReceiveOne(ctx, handler); err != nil {
			return err
		}
		if !a.Options.ReadFollow {
			return nil
		}
	}
}

// readTopic reads messages from an ASB topic using the given subscription name
func (a *ServiceBus) readTopic(ctx context.Context, handler servicebus.HandlerFunc) error {
	sub, err := a.topic.NewSubscription(a.Options.Azure.Subscription)
	if err != nil {
		return errors.Wrap(err, "unable to create topic subscription")
	}

	defer sub.Close(ctx)

	for {
		if err := sub.ReceiveOne(ctx, handler); err != nil {
			return err
		}

		if !a.Options.ReadFollow {
			return nil
		}
	}

	return nil
}

// validateReadOptions ensures the correct CLI options are specified for the read action
func validateReadOptions(opts *options.Options) error {
	if opts.Azure.Topic != "" && opts.Azure.Queue != "" {
		return errTopicOrQueue
	}

	if opts.Azure.Topic != "" && opts.Azure.Subscription == "" {
		return errMissingSubscription
	}
	return nil
}
