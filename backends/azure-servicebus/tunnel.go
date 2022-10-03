package azure_servicebus

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/tunnel"
	"github.com/batchcorp/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (a *AzureServiceBus) Tunnel(ctx context.Context, tunnelOpts *opts.TunnelOptions, tunnelSvc tunnel.ITunnel, errorCh chan<- *records.ErrorRecord) error {
	if err := validateTunnelOptions(tunnelOpts); err != nil {
		return errors.Wrap(err, "invalid tunnel options")
	}

	llog := logrus.WithField("pkg", "azure/tunnel")

	var queueOrTopic string

	if tunnelOpts.AzureServiceBus.Args.Queue != "" {
		queueOrTopic = tunnelOpts.AzureServiceBus.Args.Queue
	} else {
		queueOrTopic = tunnelOpts.AzureServiceBus.Args.Topic
	}

	sender, err := a.client.NewSender(queueOrTopic, nil)
	if err != nil {
		return errors.Wrap(err, "unable to create new azure service bus sender client")
	}

	defer func() {
		_ = sender.Close(ctx)
	}()

	if err = tunnelSvc.Start(ctx, "Azure Service Bus", errorCh); err != nil {
		return errors.Wrap(err, "unable to create tunnel")
	}

	outboundCh := tunnelSvc.Read()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-outboundCh:
			msg := &azservicebus.Message{Body: outbound.Blob}

			if err = sender.SendMessage(ctx, msg, nil); err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				break
			}

			llog.Debugf("Replayed message to Azure Service Bus for replay '%s'", outbound.ReplayId)
		case <-ctx.Done():
			llog.Debug("context cancelled")
			return nil
		}
	}
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
