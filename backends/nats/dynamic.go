package nats

import (
	"context"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/dynamic"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

func (n *Nats) Dynamic(ctx context.Context, dynamicOpts *opts.DynamicOptions) error {
	if err := validateDynamicOptions(dynamicOpts); err != nil {
		return errors.Wrap(err, "unable to validate dynamic options")
	}

	// Start up dynamic connection
	grpc, err := dynamic.New(dynamicOpts, BackendName)
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	subject := dynamicOpts.Nats.Args.Subject

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			if err := n.Client.Publish(subject, outbound.Blob); err != nil {
				n.log.Errorf("Unable to replay message: %s", err)
				break
			}

			n.log.Debugf("Replayed message to Nats topic '%s' for replay '%s'", subject, outbound.ReplayId)
		}
	}

	return nil
}

func validateDynamicOptions(dynamicOpts *opts.DynamicOptions) error {
	if dynamicOpts == nil {
		return errors.New("write options cannot be nil")
	}

	if dynamicOpts.Nats == nil {
		return errors.New("backend group options cannot be nil")
	}

	if dynamicOpts.Nats.Args == nil {
		return errors.New("backend arg options cannot be nil")
	}

	if dynamicOpts.Nats.Args.Subject == "" {
		return ErrMissingSubject
	}

	return nil
}
