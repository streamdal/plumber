package nats

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/backends/nats/types"
	"github.com/batchcorp/plumber/prometheus"
	"github.com/batchcorp/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (n *Nats) Relay(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}, errorCh chan *records.ErrorRecord) error {
	if err := validateRelayOptions(relayOpts); err != nil {
		return errors.Wrap(err, "unable to validate relay options")
	}

	n.Client.Subscribe(relayOpts.Nats.Args.Subject, func(msg *nats.Msg) {
		relayCh <- &types.RelayMessage{
			Value:   msg,
			Options: &types.RelayMessageOptions{},
		}

		prometheus.Incr("nats-relay-consumer", 1)
	})

	<-ctx.Done()
	n.log.Info("Received shutdown signal, existing relayer")

	return nil

}

// validateRelayOptions ensures all required relay options are present
func validateRelayOptions(relayOpts *opts.RelayOptions) error {
	if relayOpts == nil {
		return validate.ErrEmptyRelayOpts
	}

	if relayOpts.Nats == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := relayOpts.Nats.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.Subject == "" {
		return ErrMissingSubject
	}

	return nil
}
