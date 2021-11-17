package awssqs

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/dynamic"
)

func (a *AWSSQS) Dynamic(ctx context.Context, opts *opts.DynamicOptions) error {
	llog := logrus.WithField("pkg", "awssqs/dynamic")

	if err := validateDynamicOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate dynamic options")
	}

	args := opts.Awssqs.Args

	queueURL, err := a.getQueueURL(args.QueueName, args.RemoteAccountId)
	if err != nil {
		return errors.Wrap(err, "unable to get queue url")
	}

	// Start up dynamic connection
	grpc, err := dynamic.New(opts, BackendName)
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

MAIN:
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			// write
			if err := a.writeMsg(args, string(outbound.Blob), queueURL); err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				break MAIN
			}
		case <-ctx.Done():
			break MAIN
		}
	}

	return nil
}

func validateDynamicOptions(opts *opts.DynamicOptions) error {
	if opts == nil {
		return errors.New("dynamic options cannot be nil")
	}

	if opts.Awssqs == nil {
		return errors.New("backend group options cannot be nil")
	}

	if opts.Awssqs.Args == nil {
		return errors.New("backend arg options cannot be nil")
	}

	if opts.Awssqs.Args.QueueName == "" {
		return ErrMissingQueue
	}

	return nil
}
