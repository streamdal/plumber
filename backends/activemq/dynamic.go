package activemq

import (
	"context"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/dynamic"
	"github.com/batchcorp/plumber/validate"
)

func (a *ActiveMQ) Dynamic(ctx context.Context, dynamicOpts *opts.DynamicOptions, dynamicSvc dynamic.IDynamic, errorCh chan<- *records.ErrorRecord) error {
	if err := validateDynamicOptions(dynamicOpts); err != nil {
		return errors.Wrap(err, "invalid dynamic options")
	}

	llog := a.log.WithField("pkg", "activemq/dynamic")

	if err := dynamicSvc.Start(ctx, "ActiveMQ", errorCh); err != nil {
		return errors.Wrap(err, "unable to create dynamic")
	}

	destination := getDestinationWrite(dynamicOpts.Activemq.Args)

	outboundCh := dynamicSvc.Read()

	for {
		select {
		case outbound := <-outboundCh:
			if err := a.client.Send(destination, "", outbound.Blob, nil); err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				break
			}

			llog.Debugf("Replayed message to ActiveMQ '%s' for replay '%s'", destination, outbound.ReplayId)
		case <-ctx.Done():
			a.log.Debug("context cancelled")
			return nil
		}
	}

	return nil
}

func validateDynamicOptions(dynamicOpts *opts.DynamicOptions) error {
	if dynamicOpts == nil {
		return validate.ErrEmptyDynamicOpts
	}

	if dynamicOpts.Activemq == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := dynamicOpts.Activemq.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.Queue == "" && args.Topic == "" {
		return ErrTopicOrQueue
	}

	if args.Queue != "" && args.Topic != "" {
		return ErrTopicAndQueue
	}

	return nil
}
