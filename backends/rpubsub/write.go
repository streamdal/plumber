package rpubsub

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

func (r *RedisPubsub) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan<- *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "invalid write options")
	}

	for _, msg := range messages {
		for _, ch := range writeOpts.RedisPubsub.Args.Channels {
			cmd := r.client.Publish(ctx, ch, msg.Input)
			if cmd.Err() != nil {
				util.WriteError(nil, errorCh, fmt.Errorf("unable to publish redis pubsub message: %s", cmd.Err()))
			}
		}
	}

	return nil
}

func validateWriteOptions(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return validate.ErrEmptyWriteOpts
	}

	if writeOpts.RedisPubsub == nil {
		return validate.ErrEmptyBackendGroup
	}

	if writeOpts.RedisPubsub.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if len(writeOpts.RedisPubsub.Args.Channels) == 0 {
		return ErrMissingChannel
	}

	return nil
}
