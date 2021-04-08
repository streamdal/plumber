package azure_eventhub

import (
	"context"
	eventhub "github.com/Azure/azure-event-hubs-go/v3"

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
		return errors.Wrap(err, "unable to validate write options")
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

	a := &AzureEventHub{
		Options: opts,
		MsgDesc: md,
		Client:  client,
		log:     logrus.WithField("pkg", "azure-eventhub/write.go"),
	}

	return a.Write(ctx, msg)
}

// Write writes a message to a random partition on
func (a *AzureEventHub) Write(ctx context.Context, value []byte) error {
	opts := make([]eventhub.SendOption, 0)

	if a.Options.AzureEventHub.MessageID != "" {
		opts = append(opts, eventhub.SendWithMessageID(a.Options.AzureEventHub.MessageID))
	}

	event := eventhub.NewEvent(value)
	if a.Options.AzureEventHub.PartitionKey != "" {
		event.PartitionKey = &a.Options.AzureEventHub.PartitionKey
	}

	return a.Client.Send(ctx, event, opts...)
}

// validateWriteOptions ensures the correct CLI options are specified for the write action
func validateWriteOptions(opts *cli.Options) error {
	return nil
}
