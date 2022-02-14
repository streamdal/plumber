package azure_eventhub

import (
	"context"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/tunnel"
	"github.com/batchcorp/plumber/validate"
)

func (a *AzureEventHub) Tunnel(ctx context.Context, tunnelOpts *opts.TunnelOptions, tunnelSvc tunnel.ITunnel, errorCh chan<- *records.ErrorRecord) error {
	if err := validateTunnelOptions(tunnelOpts); err != nil {
		return errors.Wrap(err, "invalid tunnel options")
	}

	llog := logrus.WithField("pkg", "azure-eventhub/tunnel")

	if err := tunnelSvc.Start(ctx, "Azure Event Hub", errorCh); err != nil {
		return errors.Wrap(err, "unable to create tunnel")
	}

	sendOpts := make([]eventhub.SendOption, 0)

	outboundCh := tunnelSvc.Read()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-outboundCh:

			event := eventhub.NewEvent(outbound.Blob)
			if tunnelOpts.AzureEventHub.Args.PartitionKey != "" {
				event.PartitionKey = &tunnelOpts.AzureEventHub.Args.PartitionKey
			}

			if err := a.client.Send(ctx, event, sendOpts...); err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				break
			}

			llog.Debugf("Replayed message to Azure Event Hub for replay '%s'", outbound.ReplayId)
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

	if tunnelOpts.AzureEventHub == nil {
		return validate.ErrEmptyBackendGroup
	}

	if tunnelOpts.AzureEventHub.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	return nil
}
