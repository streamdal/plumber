package nats_jetstream

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	"github.com/streamdal/plumber/backends/nats-jetstream/types"
	"github.com/streamdal/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (n *NatsJetstream) Relay(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}, _ chan<- *records.ErrorRecord) error {
	if err := validateRelayOptions(relayOpts); err != nil {
		return errors.Wrap(err, "unable to validate relay options")
	}

	jsCtx, err := n.client.JetStream()
	if err != nil {
		return errors.Wrap(err, "failed to get jetstream context")
	}

	// TODO: This should be a pull subscriber
	jsCtx.Subscribe(relayOpts.NatsJetstream.Args.Stream, func(msg *nats.Msg) {
		relayCh <- &types.RelayMessage{
			Value: msg,
			Options: &types.RelayMessageOptions{
				Stream: relayOpts.NatsJetstream.Args.Stream,
			},
		}
	})

	<-ctx.Done()

	return nil
}

// validateRelayOptions ensures all required relay options are present
func validateRelayOptions(relayOpts *opts.RelayOptions) error {
	if relayOpts == nil {
		return validate.ErrEmptyRelayOpts
	}

	if relayOpts.NatsJetstream == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := relayOpts.NatsJetstream.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.Stream == "" {
		return ErrMissingStream
	}

	return nil
}
