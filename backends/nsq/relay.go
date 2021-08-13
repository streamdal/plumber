package nsq

import (
	"context"

	ntypes "github.com/batchcorp/plumber/backends/nsq/types"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/stats"
	"github.com/batchcorp/plumber/types"
	"github.com/nsqio/go-nsq"
	"github.com/pkg/errors"
)

// Relay reads messages from NSQ and sends them to RelayCh which is then read by relay.Run()
func (n *NSQ) Relay(ctx context.Context, relayCh chan interface{}, errorCh chan *types.ErrorMessage) error {
	if err := validateRelayOptions(n.Options); err != nil {
		return errors.Wrap(err, "unable to verify options")
	}

	n.log.Infof("Relaying NSQ messages from topic '%s', channel '%s' -> %s",
		n.Options.NSQ.Topic, n.Options.NSQ.Channel, n.Options.Relay.GRPCAddress)

	n.log.Infof("HTTP server listening on '%s'", n.Options.Relay.HTTPListenAddress)

	n.consumer.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
		stats.Incr("nsq-relay-consumer", 1)

		n.log.Debugf("Writing NSQ message to relay channel: %s", string(msg.Body))

		relayCh <- &ntypes.RelayMessage{
			Value: msg,
			Options: &ntypes.RelayMessageOptions{
				Topic:   n.Options.NSQ.Topic,
				Channel: n.Options.NSQ.Channel,
			},
		}

		return nil
	}))

MAIN:
	for {
		select {
		case <-ctx.Done():
			n.log.Info("Received shutdown signal, existing relayer")
			break MAIN
		default:
			// noop
		}
	}

	n.log.Debug("exiting")

	return nil
}

func validateRelayOptions(opts *options.Options) error {
	// These currently accept the same params
	return validateReadOptions(opts)
}
