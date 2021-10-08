package rabbitmq

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/util"
)

func (r *RabbitMQ) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "unable to verify write options")
	}

	producer, err := r.newRabbitForWrite(writeOpts.Rabbit.Args)
	if err != nil {
		return errors.Wrap(err, "unable to create new producer")
	}

	defer producer.Close()

	rk := writeOpts.Rabbit.Args.RoutingKey

	for _, msg := range messages {
		if err := producer.Publish(ctx, rk, []byte(msg.Input)); err != nil {
			util.WriteError(r.log, errorCh, fmt.Errorf("unable to write message to '%s': %s", rk, err))
		}
	}

	return nil
}

func validateWriteOptions(opts *opts.WriteOptions) error {
	if opts == nil {
		return errors.New("write options cannot be nil")
	}

	if opts.Rabbit == nil {
		return errors.New("backend group options cannot be nil")
	}

	if opts.Rabbit.Args == nil {
		return errors.New("backend arg options cannot be nil")
	}

	return nil
}
