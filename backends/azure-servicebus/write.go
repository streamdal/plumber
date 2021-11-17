package azure_servicebus

import (
	"context"
	"fmt"
	"time"

	servicebus "github.com/Azure/azure-service-bus-go"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/validate"
)

func (a *AzureServiceBus) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "invalid write options")
	}

	if writeOpts.AzureServiceBus.Args.Queue != "" {
		queue, err := a.client.NewQueue(writeOpts.AzureServiceBus.Args.Queue)
		if err != nil {
			return errors.Wrap(err, "unable to create new azure service bus queue client")
		}

		defer queue.Close(ctx)

		for _, msg := range messages {
			msg := servicebus.NewMessage([]byte(msg.Input))
			if err := queue.Send(ctx, msg); err != nil {
				errorCh <- &records.ErrorRecord{
					Error:               fmt.Sprintf("unable to publish message to azure service bus queue: %s", err),
					OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
				}
			}
		}

		return nil

	}

	// Topic
	topic, err := a.client.NewTopic(writeOpts.AzureServiceBus.Args.Topic)
	if err != nil {
		return errors.Wrap(err, "unable to create new azure service bus topic client")
	}

	defer topic.Close(ctx)

	for _, msg := range messages {
		msg := servicebus.NewMessage([]byte(msg.Input))
		if err := topic.Send(ctx, msg); err != nil {
			errorCh <- &records.ErrorRecord{
				Error:               fmt.Sprintf("unable to publish message to azure service bus topic: %s", err),
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
