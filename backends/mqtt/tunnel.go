package mqtt

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/tunnel"
	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

func (m *MQTT) Tunnel(ctx context.Context, tunnelOpts *opts.TunnelOptions, tunnelSvc tunnel.ITunnel, errorCh chan<- *records.ErrorRecord) error {
	if err := validateTunnelOptions(tunnelOpts); err != nil {
		return errors.Wrap(err, "invalid tunnel options")
	}

	llog := m.log.WithField("pkg", "mqtt/tunnel")

	if err := tunnelSvc.Start(ctx, "MQTT", errorCh); err != nil {
		return errors.Wrap(err, "unable to create tunnel")
	}

	timeout := util.DurationSec(tunnelOpts.Mqtt.Args.WriteTimeoutSeconds)
	topic := tunnelOpts.Mqtt.Args.Topic

	outboundCh := tunnelSvc.Read()

	for {
		select {
		case outbound := <-outboundCh:
			token := m.client.Publish(topic, byte(int(m.connArgs.QosLevel)), false, outbound.Blob)

			if !token.WaitTimeout(timeout) {
				return fmt.Errorf("timed out attempting to publish message after %d seconds",
					tunnelOpts.Mqtt.Args.WriteTimeoutSeconds)
			}

			if token.Error() != nil {
				return errors.Wrap(token.Error(), "unable to replay message")
			}

			llog.Debugf("Replayed message to MQTT topic '%s' for replay '%s'", topic, outbound.ReplayId)
		case <-ctx.Done():
			m.log.Debug("context cancelled")
			return nil
		}
	}
}

func validateTunnelOptions(tunnelOpts *opts.TunnelOptions) error {
	if tunnelOpts == nil {
		return validate.ErrEmptyTunnelOpts
	}

	if tunnelOpts.Mqtt == nil {
		return validate.ErrEmptyBackendGroup
	}

	if tunnelOpts.Mqtt.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if tunnelOpts.Mqtt.Args.Topic == "" {
		return ErrEmptyTopic
	}

	return nil
}
