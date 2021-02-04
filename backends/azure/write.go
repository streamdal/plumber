package azure

import (
	"context"

	servicebus "github.com/Azure/azure-service-bus-go"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/writer"
)

// Write performs necessary setup and calls AzureServiceBus.Write() to write the actual message
func Write(opts *cli.Options) error {
	ctx := context.Background()

	if err := writer.ValidateWriteOptions(opts, validateWriteOptions); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	var mdErr error
	var md *desc.MessageDescriptor

	if opts.WriteInputType == "jsonpb" {
		md, mdErr = pb.FindMessageDescriptor(opts.WriteProtobufDirs, opts.WriteProtobufRootMessage)
		if mdErr != nil {
			return errors.Wrap(mdErr, "unable to find root message descriptor")
		}
	}

	msg, err := writer.GenerateWriteValue(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	a := &AzureServiceBus{
		Options: opts,
		MsgDesc: md,
		Client:  client,
		log:     logrus.WithField("pkg", "azure/write.go"),
	}

	if opts.Azure.Queue != "" {
		queue, err := client.NewQueue(opts.Azure.Queue)
		if err != nil {
			return errors.Wrap(err, "unable to create new azure service bus queue client")
		}

		defer queue.Close(ctx)

		a.Queue = queue
	} else {
		topic, err := client.NewTopic(opts.Azure.Topic)
		if err != nil {
			return errors.Wrap(err, "unable to create new azure service bus topic client")
		}

		defer topic.Close(ctx)

		a.Topic = topic
	}

	return a.Write(ctx, msg)
}

// Write writes a message to an ASB topic or queue, depending on which is specified
func (a *AzureServiceBus) Write(ctx context.Context, value []byte) error {
	if a.Options.Azure.Queue != "" {
		return a.writeToQueue(ctx, value)
	}

	return a.writeToTopic(ctx, value)
}

// writeToQueue writes the message to an ASB queue
func (a *AzureServiceBus) writeToQueue(ctx context.Context, value []byte) error {
	msg := servicebus.NewMessage(value)
	if err := a.Queue.Send(ctx, msg); err != nil {
		return errors.Wrap(err, "message could not be published to queue")
	}

	a.log.Infof("Write message to queue '%s'", a.Client.Name)

	return nil
}

// writeToTopic writes a message to an ASB topic
func (a *AzureServiceBus) writeToTopic(ctx context.Context, value []byte) error {
	msg := servicebus.NewMessage(value)
	if err := a.Topic.Send(ctx, msg); err != nil {
		return errors.Wrap(err, "message could not be published to topic")
	}

	a.log.Infof("Write message to topic '%s'", a.Client.Name)

	return nil
}

// validateWriteOptions ensures the correct CLI options are specified for the write action
func validateWriteOptions(opts *cli.Options) error {
	if opts.Azure.Topic != "" && opts.Azure.Queue != "" {
		return errTopicOrQueue
	}
	return nil
}
