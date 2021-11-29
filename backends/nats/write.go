package nats

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

func (n *Nats) Write(_ context.Context, writeOpts *opts.WriteOptions, errorCh chan *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	defer n.Client.Close()

	subject := writeOpts.Nats.Args.Subject

	for _, msg := range messages {
		if err := n.Client.Publish(subject, []byte(msg.Input)); err != nil {
			util.WriteError(n.log, errorCh, fmt.Errorf("unable to publish message to subject '%s': %s", subject, err))
			continue
		}
		return nil
	}

	return nil
}

func validateWriteOptions(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return validate.ErrEmptyWriteOpts
	}

	if writeOpts.Nats == nil {
		return validate.ErrEmptyBackendGroup
	}

	if writeOpts.Nats.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if writeOpts.Nats.Args.Subject == "" {
		return ErrMissingSubject
	}

	return nil
}
