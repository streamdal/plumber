package azure_eventhub

import (
	"context"
	"fmt"
	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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

	a := &AzureEventHub{
		Options: opts,
		MsgDesc: md,
		Client:  client,
		printer: printer.New(),
		log:     logrus.WithField("pkg", "azure-eventhub/read.go"),
	}

	return a.Read()
}

func (a *AzureEventHub) Read() error {
	ctx := context.Background()

	defer a.Client.Close(ctx)

	a.log.Info("Listening for message(s) ...")

	lineNumber := 1

	var hasRead bool

	handler := func(c context.Context, event *eventhub.Event) error {
		data, err := reader.Decode(a.Options, a.MsgDesc, event.Data)
		if err != nil {
			return err
		}

		str := string(data)

		if a.Options.ReadLineNumbers {
			str = fmt.Sprintf("%d: ", lineNumber) + str
			lineNumber++
		}

		a.printer.Print(str)

		hasRead = true

		return nil
	}

	runtimeInfo, err := a.Client.GetRuntimeInformation(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to get azure eventhub partition list")
	}

	for {
		for _, partitionID := range runtimeInfo.PartitionIDs {
			// Start receiving messages
			//
			// Receive blocks while attempting to connect to hub, then runs until listenerHandle.Close() is called
			// <- listenerHandle.Done() signals listener has stopped
			// listenerHandle.Err() provides the last error the receiver encountered
			listenerHandle, err := a.Client.Receive(ctx, partitionID, handler, eventhub.ReceiveWithLatestOffset())
			if err != nil {
				return errors.Wrap(err, "unable to receive message from azure eventhub")
			}

			if !a.Options.ReadFollow && hasRead {
				listenerHandle.Close(ctx)
				return nil
			}
		}
	}

	return nil
}

// validateReadOptions ensures the correct CLI options are specified for the read action
func validateReadOptions(opts *cli.Options) error {
	return nil
}
