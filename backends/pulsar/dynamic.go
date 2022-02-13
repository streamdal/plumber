package pulsar

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/dynamic"
	"github.com/batchcorp/plumber/validate"
)

func (p *Pulsar) Dynamic(ctx context.Context, dynamicOpts *opts.DynamicOptions, dynamicSvc dynamic.IDynamic, errorCh chan<- *records.ErrorRecord) error {
	if err := validateDynamicOptions(dynamicOpts); err != nil {
		return errors.Wrap(err, "invalid dynamic options")
	}

	llog := p.log.WithField("pkg", "pulsar/dynamic")

	producer, err := p.client.CreateProducer(pulsar.ProducerOptions{Topic: dynamicOpts.Pulsar.Args.Topic})
	if err != nil {
		return errors.Wrap(err, "unable to create Pulsar producer")
	}

	if err := dynamicSvc.Start(ctx, "Apache Pulsar", errorCh); err != nil {
		return errors.Wrap(err, "unable to create dynamic")
	}

	outboundCh := dynamicSvc.Read()

	for {
		select {
		case outbound := <-outboundCh:
			if _, err := producer.Send(ctx, &pulsar.ProducerMessage{Payload: outbound.Blob}); err != nil {
				err = errors.Wrap(err, "Unable to replay message")
				llog.Error(err)
				return err
			}

			llog.Debugf("Replayed message to Pulsar topic '%s' for replay '%s'",
				dynamicOpts.Pulsar.Args.Topic, outbound.ReplayId)
		case <-ctx.Done():
			llog.Debug("context cancelled")
			return nil
		}
	}
}

func validateDynamicOptions(dynamicOpts *opts.DynamicOptions) error {
	if dynamicOpts == nil {
		return validate.ErrEmptyDynamicOpts
	}

	if dynamicOpts.Pulsar == nil {
		return validate.ErrEmptyBackendGroup
	}

	if dynamicOpts.Pulsar.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if dynamicOpts.Pulsar.Args.Topic == "" {
		return ErrEmptyTopic
	}

	return nil
}
