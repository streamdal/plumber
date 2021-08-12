package azure

import (
	"context"

	servicebus "github.com/Azure/azure-service-bus-go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	atypes "github.com/batchcorp/plumber/backends/azure/types"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/stats"
	ptypes "github.com/batchcorp/plumber/types"
)

type Relayer struct {
	Options     *options.Options
	RelayCh     chan interface{}
	log         *logrus.Entry
	Queue       *servicebus.Queue
	Topic       *servicebus.Topic
	ShutdownCtx context.Context
}

func (s *ServiceBus) Relay(ctx context.Context, relayCh chan interface{}, errorCh chan *ptypes.ErrorMessage) error {
	r := &Relayer{
		Options:     s.Options,
		RelayCh:     relayCh,
		ShutdownCtx: ctx,
		Queue:       s.queue,
		Topic:       s.topic,
		log:         logrus.WithField("pkg", "azure/relay.go"),
	}

	return r.Relay()
}

func (r *Relayer) Relay() error {
	if r.Queue != nil {
		r.log.Infof("Relaying azure service bus messages from '%s' queue -> '%s'",
			r.Options.Azure.Queue, r.Options.Relay.GRPCAddress)
	} else {
		r.log.Infof("Relaying azure service bus messages from '%s' topic -> '%s'",
			r.Options.Azure.Topic, r.Options.Relay.GRPCAddress)
	}

	r.log.Infof("HTTP server listening on '%s'", r.Options.Relay.HTTPListenAddress)

	var handler servicebus.HandlerFunc = func(ctx context.Context, msg *servicebus.Message) error {
		r.log.Debug("Writing message to relay channel")

		// This might be nil if no user properties were sent with the original message
		if msg.UserProperties == nil {
			msg.UserProperties = make(map[string]interface{}, 0)
		}

		// Azure's Message struct does not include this information for some reason
		// Seems like it would be good to have. Prefixed with plumber to avoid collisions with user data
		if r.Queue != nil {
			msg.UserProperties["plumber_queue"] = r.Options.Azure.Queue
		} else {
			msg.UserProperties["plumber_topic"] = r.Options.Azure.Topic
			msg.UserProperties["plumber_subscription"] = r.Options.Azure.Subscription
		}

		stats.Incr("azure-relay-consumer", 1)

		r.RelayCh <- &atypes.RelayMessage{
			Value: msg,
		}

		if err := msg.Complete(ctx); err != nil {
			r.log.Warningf("unable to mark message as complete: %s", err)
		}

		return nil
	}

	if r.Queue != nil {
		return r.readQueue(handler)
	}

	return r.readTopic(handler)
}

// readQueue reads messages from an ASB queue
func (r *Relayer) readQueue(handler servicebus.HandlerFunc) error {
	defer r.Queue.Close(r.ShutdownCtx)

	for {
		if err := r.Queue.ReceiveOne(r.ShutdownCtx, handler); err != nil {
			if err == context.Canceled {
				r.log.Info("Received shutdown signal, existing relayer")
				return nil
			}

			stats.IncrPromCounter("plumber_read_errors", 1)

			return err
		}
	}
}

// readTopic reads messages from an ASB topic using the given subscription name
func (r *Relayer) readTopic(handler servicebus.HandlerFunc) error {
	sub, err := r.Topic.NewSubscription(r.Options.Azure.Subscription)
	if err != nil {
		return errors.Wrap(err, "unable to create topic subscription")
	}

	defer sub.Close(r.ShutdownCtx)

	for {
		if err := sub.ReceiveOne(r.ShutdownCtx, handler); err != nil {
			if err == context.Canceled {
				r.log.Info("Received shutdown signal, existing relayer")
				return nil
			}

			stats.IncrPromCounter("plumber_read_errors", 1)

			return err
		}
	}
}
