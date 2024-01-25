package nats_streaming

import (
	"context"
	"time"

	"github.com/nats-io/stan.go"
	pb2 "github.com/nats-io/stan.go/pb"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/backends/nats-streaming/types"
	"github.com/streamdal/plumber/prometheus"
	"github.com/streamdal/plumber/validate"
)

func (n *NatsStreaming) Relay(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}, errorCh chan<- *records.ErrorRecord) error {
	if err := validateRelayOptions(relayOpts); err != nil {
		return errors.Wrap(err, "invalid read options")
	}

	var subFunc = func(msg *stan.Msg) {
		prometheus.Incr("nats-streaming-relay-consumer", 1)
		relayCh <- &types.RelayMessage{
			Value: msg,
			Options: &types.RelayMessageOptions{
				Channel: relayOpts.NatsStreaming.Args.Channel,
			},
		}
	}

	subOptions, err := n.getRelayReadOptions(relayOpts)
	if err != nil {
		return errors.Wrap(err, "unable to read from nats-streaming")
	}

	sub, err := n.stanClient.Subscribe(relayOpts.NatsStreaming.Args.Channel, subFunc, subOptions...)
	if err != nil {
		return errors.Wrap(err, "unable to create subscription")
	}

	defer sub.Unsubscribe()

	<-ctx.Done()
	n.log.Debug("Received shutdown signal, exiting relayer")

	return nil
}

// getReadOptions returns slice of options to pass to Stan.io. Only one read option of --last, --since, --seq, --new--only
// is allowed, so return early once we have a read option
func (n *NatsStreaming) getRelayReadOptions(readOpts *opts.RelayOptions) ([]stan.SubscriptionOption, error) {
	opts := make([]stan.SubscriptionOption, 0)

	if readOpts.NatsStreaming.Args.DurableName != "" {
		opts = append(opts, stan.DurableName(readOpts.NatsStreaming.Args.DurableName))
	}

	if readOpts.NatsStreaming.Args.ReadAll {
		opts = append(opts, stan.DeliverAllAvailable())
		return opts, nil
	}

	if readOpts.NatsStreaming.Args.ReadLastAvailable {
		opts = append(opts, stan.StartWithLastReceived())
		return opts, nil
	}

	if readOpts.NatsStreaming.Args.ReadSince != "" {
		t, err := time.ParseDuration(readOpts.NatsStreaming.Args.ReadSince)
		if err != nil {
			return nil, errors.Wrap(err, "unable to parse --since option")
		}

		opts = append(opts, stan.StartAtTimeDelta(t))
		return opts, nil
	}

	// TODO: change proto type so we don't have to typecast
	if readOpts.NatsStreaming.Args.ReadSequenceNumber > 0 {
		opts = append(opts, stan.StartAtSequence(uint64(readOpts.NatsStreaming.Args.ReadSequenceNumber)))
		return opts, nil
	}

	// Default option is new-only
	opts = append(opts, stan.StartAt(pb2.StartPosition_NewOnly))

	return opts, nil
}

// validateRelayOptions ensures all required relay options are present
func validateRelayOptions(relayOpts *opts.RelayOptions) error {
	if relayOpts == nil {
		return validate.ErrEmptyRelayOpts
	}

	if relayOpts.NatsStreaming == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := relayOpts.NatsStreaming.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.Channel == "" {
		return ErrEmptyChannel
	}

	return nil
}
