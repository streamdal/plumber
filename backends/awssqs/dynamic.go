package awssqs

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/dynamic"
	"github.com/batchcorp/plumber/validate"
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
	grpc, err := dynamic.New(opts, "AWS SQS")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			// write
			if err := a.writeMsg(args, string(outbound.Blob), queueURL); err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				return nil
			}
		case <-ctx.Done():
			return nil
		}
	}

	return nil
}

func validateDynamicOptions(dynamicOpts *opts.DynamicOptions) error {
	if dynamicOpts == nil {
		return validate.ErrEmptyDynamicOpts
	}

	if dynamicOpts.Awssqs == nil {
		return validate.ErrEmptyBackendGroup
	}

	if dynamicOpts.Awssqs.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if dynamicOpts.Awssqs.Args.QueueName == "" {
		return ErrMissingQueue
	}

	return nil
}
