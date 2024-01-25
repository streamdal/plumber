package azure_servicebus

import (
	"context"

	serviceBus "github.com/Azure/azure-service-bus-go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/streamdal/plumber/tunnel"
	"github.com/streamdal/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (a *AzureServiceBus) Tunnel(ctx context.Context, tunnelOpts *opts.TunnelOptions, tunnelSvc tunnel.ITunnel, errorCh chan<- *records.ErrorRecord) error {
	if err := validateTunnelOptions(tunnelOpts); err != nil {
		return errors.Wrap(err, "invalid tunnel options")
	}

	llog := logrus.WithField("pkg", "azure/tunnel")

	var queue *serviceBus.Queue
	var topic *serviceBus.Topic
	var err error

	if tunnelOpts.AzureServiceBus.Args.Queue != "" {
		queue, err = a.client.NewQueue(tunnelOpts.AzureServiceBus.Args.Queue)
		if err != nil {
			return errors.Wrap(err, "unable to create new azure service bus queue client")
		}

		defer queue.Close(ctx)
	} else {
		topic, err = a.client.NewTopic(tunnelOpts.AzureServiceBus.Args.Topic)
		if err != nil {
			return errors.Wrap(err, "unable to create new azure service bus topic client")
		}

		defer topic.Close(ctx)
	}

	if err := tunnelSvc.Start(ctx, "Azure Service Bus", errorCh); err != nil {
		return errors.Wrap(err, "unable to create tunnel")
	}

	outboundCh := tunnelSvc.Read()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-outboundCh:
			msg := serviceBus.NewMessage(outbound.Blob)

			if queue != nil {
				// Publishing to queue
				if err := queue.Send(ctx, msg); err != nil {
					llog.Errorf("Unable to replay message: %s", err)
					break
				}
			} else {
				// Publishing to topic
				if err := topic.Send(ctx, msg); err != nil {
					llog.Errorf("Unable to replay message: %s", err)
					break
				}
			}

			llog.Debugf("Replayed message to Azure Service Bus for replay '%s'", outbound.ReplayId)
		case <-ctx.Done():
			llog.Debug("context cancelled")
			return nil
		}
	}

	return nil
}

func validateTunnelOptions(tunnelOpts *opts.TunnelOptions) error {
	if tunnelOpts == nil {
		return validate.ErrEmptyTunnelOpts
	}

	if tunnelOpts.AzureServiceBus == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := tunnelOpts.AzureServiceBus.Args
	if tunnelOpts.AzureServiceBus.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.Queue == "" && args.Topic == "" {
		return ErrQueueOrTopic
	}

	if args.Queue != "" && args.Topic != "" {
		return ErrQueueAndTopic
	}

	return nil
}
