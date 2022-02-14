package pulsar

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/tunnel"
	"github.com/batchcorp/plumber/validate"
)

func (p *Pulsar) Tunnel(ctx context.Context, tunnelOpts *opts.DynamicOptions, dynamicSvc tunnel.ITunnel, errorCh chan<- *records.ErrorRecord) error {
	if err := validateTunnelOptions(tunnelOpts); err != nil {
		return errors.Wrap(err, "invalid tunnel options")
	}

	llog := p.log.WithField("pkg", "pulsar/tunnel")

	producer, err := p.client.CreateProducer(pulsar.ProducerOptions{Topic: tunnelOpts.Pulsar.Args.Topic})
	if err != nil {
		return errors.Wrap(err, "unable to create Pulsar producer")
	}

	if err := dynamicSvc.Start(ctx, "Apache Pulsar", errorCh); err != nil {
		return errors.Wrap(err, "unable to create tunnel")
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
				tunnelOpts.Pulsar.Args.Topic, outbound.ReplayId)
		case <-ctx.Done():
			llog.Debug("context cancelled")
			return nil
		}
	}
}

func validateTunnelOptions(tunnelOpts *opts.DynamicOptions) error {
	if tunnelOpts == nil {
		return validate.ErrEmptyDynamicOpts
	}

	if tunnelOpts.Pulsar == nil {
		return validate.ErrEmptyBackendGroup
	}

	if tunnelOpts.Pulsar.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if tunnelOpts.Pulsar.Args.Topic == "" {
		return ErrEmptyTopic
	}

	return nil
}
