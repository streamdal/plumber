package nats_jetstream

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (n *NatsJetstream) Write(_ context.Context, writeOpts *opts.WriteOptions, errorCh chan<- *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "invalid write options")
	}

	jsCtx, err := n.client.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		return errors.Wrap(err, "failed to get jetstream context")
	}

	stream := writeOpts.NatsJetstream.Args.Stream

	for _, msg := range messages {
		if _, err := jsCtx.Publish(stream, []byte(msg.Input)); err != nil {
			util.WriteError(n.log, errorCh, fmt.Errorf("unable to publish message to subject '%s': %s", stream, err))
			continue
		}
	}

	return nil
}

func validateWriteOptions(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return validate.ErrEmptyWriteOpts
	}

	if writeOpts.NatsJetstream == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := writeOpts.NatsJetstream.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.Stream == "" {
		return ErrMissingStream
	}

	return nil
}
