package azure_eventhub

import (
	"context"
	"fmt"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
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

	count := 1

	var hasRead bool

	handler := func(c context.Context, event *eventhub.Event) error {
		data, err := reader.Decode(a.Options, a.MsgDesc, event.Data)
		if err != nil {
			return err
		}

		str := string(data)

		str = fmt.Sprintf("%d: ", count) + str
		count++

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
func validateReadOptions(opts *options.Options) error {
	return nil
}
