package nats_streaming

import (
	"context"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

func (n *NatsStreaming) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan<- *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "invalid write options")
	}

	for _, msg := range messages {
		err := n.stanClient.Publish(writeOpts.NatsStreaming.Args.Channel, []byte(msg.Input))
		if err != nil {
			util.WriteError(nil, errorCh, errors.Wrap(err, "unable to publish nats-streaming message"))
			break
		}
	}

	return nil
}

func validateWriteOptions(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return validate.ErrEmptyWriteOpts
	}

	if writeOpts.NatsStreaming == nil {
		return validate.ErrEmptyBackendGroup
	}

	if writeOpts.NatsStreaming.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if writeOpts.NatsStreaming.Args.Channel == "" {
		return ErrEmptyChannel
	}

	return nil
}
