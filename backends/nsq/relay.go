package nsq

import (
	"context"

	"github.com/nsqio/go-nsq"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/backends/nsq/types"
)

func (n *NSQ) Relay(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}, errorCh chan *records.ErrorRecord) error {
	if err := validateRelayOptions(relayOpts); err != nil {
		return errors.Wrap(err, "unable to verify options")
	}

	consumer, err := nsq.NewConsumer(relayOpts.Nsq.Args.Topic, relayOpts.Nsq.Args.Channel, n.config)
	if err != nil {
		return errors.Wrap(err, "Could not start NSQ consumer")
	}

	consumer.SetLogger(n.log, nsq.LogLevelError)

	count := 1

	consumer.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
		count++

		relayCh <- &types.RelayMessage{
			Value:   msg,
			Options: &types.RelayMessageOptions{},
		}

		count++
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

	for {
		select {
		case <-ctx.Done():
			n.log.Info("Received shutdown signal, existing relayer")
			return nil
		default:
			// noop
		}
	}

	return nil
}

// validateRelayOptions ensures all required relay options are present
func validateRelayOptions(relayOpts *opts.RelayOptions) error {
	if relayOpts == nil {
		return errors.New("relay opts cannot be nil")
	}

	if relayOpts.Nsq == nil {
		return errors.New("NSQ read options cannot be nil")
	}

	if relayOpts.Nsq.Args == nil {
		return errors.New("NSQ read option args cannot be nil")
	}

	return nil
}
