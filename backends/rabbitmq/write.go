package rabbitmq

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

func (r *RabbitMQ) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "unable to verify write options")
	}

	// Check if nil to allow unit testing injection into struct
	if r.client == nil {
		producer, err := r.newRabbitForWrite(writeOpts.Rabbit.Args)
		if err != nil {
			return errors.Wrap(err, "unable to create new producer")
		}
		r.client = producer
	}

	rk := writeOpts.Rabbit.Args.RoutingKey

	for _, msg := range messages {
		if err := r.client.Publish(ctx, rk, []byte(msg.Input)); err != nil {
			util.WriteError(r.log, errorCh, fmt.Errorf("unable to write message to '%s': %s", rk, err))
		}
	}

	return nil
}

func validateWriteOptions(opts *opts.WriteOptions) error {
	if opts == nil {
		return validate.ErrEmptyWriteOpts
	}

	if opts.Rabbit == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := opts.Rabbit.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.RoutingKey == "" {
		return ErrEmptyRoutingKey
	}

	if args.ExchangeName == "" {
		return ErrEmptyExchangeName
	}

	return nil
}
