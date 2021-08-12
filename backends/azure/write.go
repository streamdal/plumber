package azure

import (
	"context"

	servicebus "github.com/Azure/azure-service-bus-go"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/writer"
	"github.com/pkg/errors"
)

// Write performs necessary setup and calls ServiceBus.Write() to write the actual message
func (s *ServiceBus) Write(ctx context.Context, errorCh chan *types.ErrorMessage, messages ...*types.WriteMessage) error {
	if err := writer.ValidateWriteOptions(s.Options, validateWriteOptions); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	for _, msg := range messages {
		if err := s.write(ctx, msg.Value); err != nil {
			util.WriteError(s.log, errorCh, err)
		}
	}

	return nil
}

// Write writes a message to an ASB topic or queue, depending on which is specified
func (s *ServiceBus) write(ctx context.Context, value []byte) error {
	if s.Options.Azure.Queue != "" {
		return s.writeToQueue(ctx, value)
	}

	return s.writeToTopic(ctx, value)
}

// writeToQueue writes the message to an ASB queue
func (s *ServiceBus) writeToQueue(ctx context.Context, value []byte) error {
	msg := servicebus.NewMessage(value)

	if err := s.queue.Send(ctx, msg); err != nil {
		return errors.Wrap(err, "message could not be published to queue")
	}

	s.log.Infof("Wrote message to queue '%s'", s.client.Name)

	return nil
}

// writeToTopic writes a message to an ASB topic
func (s *ServiceBus) writeToTopic(ctx context.Context, value []byte) error {
	msg := servicebus.NewMessage(value)

	if err := s.topic.Send(ctx, msg); err != nil {
		return errors.Wrap(err, "message could not be published to topic")
	}

	s.log.Infof("Wrote message to topic '%s'", s.client.Name)

	return nil
}

// validateWriteOptions ensures the correct CLI options are specified for the write action
func validateWriteOptions(opts *options.Options) error {
	if opts.Azure.Topic != "" && opts.Azure.Queue != "" {
		return errTopicOrQueue
	}
	return nil
}
