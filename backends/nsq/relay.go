package nsq

import (
	"context"

	"github.com/nsqio/go-nsq"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends/nsq/types"
	"github.com/batchcorp/plumber/prometheus"
	"github.com/batchcorp/plumber/validate"
)

func (n *NSQ) Relay(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}, errorCh chan *records.ErrorRecord) error {
	if err := validateRelayOptions(relayOpts); err != nil {
		return errors.Wrap(err, "unable to validate relay options")
	}

	consumer, err := nsq.NewConsumer(relayOpts.Nsq.Args.Topic, relayOpts.Nsq.Args.Channel, n.config)
	if err != nil {
		return errors.Wrap(err, "Could not start NSQ consumer")
	}

	consumer.SetLogger(n.log, nsq.LogLevelError)

	consumer.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {

		prometheus.Incr("nsq-relay-consumer", 1)

		n.log.Debugf("Writing NSQ message to relay channel: %s", string(msg.Body))

		relayCh <- &types.RelayMessage{
			Value: msg,
			Options: &types.RelayMessageOptions{
				Channel: relayOpts.Nsq.Args.Channel,
				Topic:   relayOpts.Nsq.Args.Topic,
			},
		}

		return nil
	}))

	// Connect to correct server. Reading can be done directly from an NSQD server
	// or let lookupd find the correct one.
	if n.connOpts.GetNsq().LookupdAddress != "" {
		if err := consumer.ConnectToNSQLookupd(n.connOpts.GetNsq().LookupdAddress); err != nil {
			return errors.Wrap(err, "could not connect to nsqlookupd")
		}
	} else {
		if err := consumer.ConnectToNSQD(n.connOpts.GetNsq().NsqdAddress); err != nil {
			return errors.Wrap(err, "could not connect to nsqd")
		}
	}

	defer consumer.Stop()

	select {
	case <-ctx.Done():
		n.log.Debug("Received shutdown signal, exiting relayer")
		return nil
	}

	return nil
}

// validateRelayOptions ensures all required relay options are present
func validateRelayOptions(relayOpts *opts.RelayOptions) error {
	if relayOpts == nil {
		return validate.ErrEmptyRelayOpts
	}

	if relayOpts.Nsq == nil {
		return validate.ErrEmptyBackendGroup
	}

	if relayOpts.Nsq.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	return nil
}
