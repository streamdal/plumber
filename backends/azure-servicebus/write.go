package azure_servicebus

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/validate"
)

func (a *AzureServiceBus) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan<- *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "invalid write options")
	}

	var queueOrTopic string

	if writeOpts.AzureServiceBus.Args.Queue != "" {
		queueOrTopic = writeOpts.AzureServiceBus.Args.Queue
	} else {
		queueOrTopic = writeOpts.AzureServiceBus.Args.Topic
	}

	sender, err := a.client.NewSender(queueOrTopic, nil)
	if err != nil {
		return errors.Wrap(err, "unable to create new azure service bus sender client")
	}

	defer func() {
		_ = sender.Close(ctx)
	}()

	for i := range messages {
		msg := &azservicebus.Message{Body: []byte(messages[i].Input)}
		if err := sender.SendMessage(ctx, msg, nil); err != nil {
			errorCh <- &records.ErrorRecord{
				Error:               fmt.Sprintf("unable to publish message to azure service bus queue/topic: %s", err),
				OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
			}
		}
	}

	return nil
}

func validateWriteOptions(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return validate.ErrEmptyWriteOpts
	}

	if writeOpts.AzureServiceBus == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := writeOpts.AzureServiceBus.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.Queue == "" && args.Topic == "" {
		return ErrQueueOrTopic
	}

	if args.Queue != "" && args.Topic != "" {
		return ErrQueueAndTopic
	}

	return nil
}
