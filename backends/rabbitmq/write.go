package rabbitmq

import (
	"context"

	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/rabbit"
	"github.com/pkg/errors"
)

// Write is the entry point function for performing write operations in RabbitMQ.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func (r *RabbitMQ) Write(ctx context.Context, errorCh chan *types.ErrorMessage, messages ...*types.WriteMessage) error {
	rmq, err := newConnection(r.Options)
	if err != nil {
		return errors.Wrap(err, "unable to create new producer")
	}

	defer rmq.Close()

	for _, msg := range messages {
		if err := r.write(ctx, rmq, msg.Value); err != nil {
			util.WriteError(r.log, errorCh, err)
		}
	}

	return nil
}

// Write is a wrapper for amqp Publish method. We wrap it so that we can mock
// it in tests, add logging etc.
func (r *RabbitMQ) write(ctx context.Context, client *rabbit.Rabbit, value []byte) error {
	err := client.Publish(ctx, r.Options.Rabbit.RoutingKey, value)
	if err != nil {
		return errors.Wrap(err, "unable to write data to rabbit")
	}

	r.log.Infof("Successfully wrote message to exchange '%s'", r.Options.Rabbit.Exchange)
	return nil
}
