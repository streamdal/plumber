package awssqs

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/dynamic"
	"github.com/batchcorp/plumber/validate"
)

func (a *AWSSQS) Dynamic(ctx context.Context, opts *opts.DynamicOptions, dynamicSvc dynamic.IDynamic) error {
	if err := validateDynamicOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate dynamic options")
	}

	llog := a.log.WithField("pkg", "awssqs/dynamic")

	args := opts.Awssqs.Args

	queueURL, err := a.getQueueURL(args.QueueName, args.RemoteAccountId)
	if err != nil {
		return errors.Wrap(err, "unable to get queue url")
	}

	go dynamicSvc.Start("AWS SQS")

	outboundCh := dynamicSvc.Read()

	for {
		select {
		case outbound := <-outboundCh:
			// write
			if err := a.writeMsg(args, string(outbound.Blob), queueURL); err != nil {
				err = fmt.Errorf("Unable to replay message: %s", err)
				llog.Error(err)
				return err
			}
		case <-ctx.Done():
			llog.Warning("context cancelled")
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
