package azure

import (
	"context"
	"time"

	servicebus "github.com/Azure/azure-service-bus-go"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
)

func (s *ServiceBus) Read(ctx context.Context, resultCh chan *types.ReadMessage, errorCh chan *types.ErrorMessage) error {
	if err := validateReadOptions(s.Options); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	s.log.Info("Listening for message(s) ...")

	count := 1

	var handler servicebus.HandlerFunc = func(ctx context.Context, msg *servicebus.Message) error {
		resultCh <- &types.ReadMessage{
			Value:      msg.Data,
			ReceivedAt: time.Now().UTC(),
			Num:        count,
			Raw:        msg,
		}

		count++

		return msg.Complete(ctx)
	}

	if s.queue != nil {
		return s.readQueue(ctx, handler, errorCh)
	}

	if s.topic != nil {
		return s.readTopic(ctx, handler, errorCh)
	}

	return nil
}

// readQueue reads messages from an ASB queue
func (s *ServiceBus) readQueue(ctx context.Context, handler servicebus.HandlerFunc, errorChan chan *types.ErrorMessage) error {
	for {
		if err := s.queue.ReceiveOne(ctx, handler); err != nil {
			util.WriteError(s.log, errorChan, errors.Wrap(err, "unable to receive message on queue"))
			return err
		}

		if !s.Options.Read.Follow {
			return nil
		}
	}
}

// readTopic reads messages from an ASB topic using the given subscription name
func (s *ServiceBus) readTopic(ctx context.Context, handler servicebus.HandlerFunc, errorChan chan *types.ErrorMessage) error {
	sub, err := s.topic.NewSubscription(s.Options.Azure.Subscription)
	if err != nil {
		return errors.Wrap(err, "unable to create topic subscription")
	}

	defer sub.Close(ctx)

	for {
		if err := sub.ReceiveOne(ctx, handler); err != nil {
			util.WriteError(s.log, errorChan, errors.Wrap(err, "unable to receive message on topic"))
			return err
		}

		if !s.Options.Read.Follow {
			return nil
		}
	}
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
