package pulsar

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/dynamic"
	"github.com/batchcorp/plumber/validate"
)

func (p *Pulsar) Dynamic(ctx context.Context, dynamicOpts *opts.DynamicOptions) error {
	if err := validateDynamicOptions(dynamicOpts); err != nil {
		return errors.Wrap(err, "invalid dynamic options")
	}

	llog := logrus.WithField("pkg", "pulsar/dynamic")

	producer, err := p.client.CreateProducer(pulsar.ProducerOptions{Topic: dynamicOpts.Pulsar.Args.Topic})
	if err != nil {
		return errors.Wrap(err, "unable to create Pulsar producer")
	}

	// Start up dynamic connection
	grpc, err := dynamic.New(dynamicOpts, "Apache Pulsar")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			if _, err := producer.Send(ctx, &pulsar.ProducerMessage{Payload: outbound.Blob}); err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				break
			}

			llog.Debugf("Replayed message to Pulsar topic '%s' for replay '%s'",
				dynamicOpts.Pulsar.Args.Topic, outbound.ReplayId)
		case <-ctx.Done():
			p.log.Warning("context cancelled")
			return nil
		}
	}

	return nil
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
