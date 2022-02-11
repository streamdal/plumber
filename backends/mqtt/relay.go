package mqtt

import (
	"context"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends/mqtt/types"
	"github.com/batchcorp/plumber/prometheus"
	"github.com/batchcorp/plumber/validate"
)

func (m *MQTT) Relay(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}, errorCh chan *records.ErrorRecord) error {
	if err := validateRelayOptions(relayOpts); err != nil {
		return errors.Wrap(err, "unable to validate relay options")
	}

	var readFunc = func(client mqtt.Client, msg mqtt.Message) {
		prometheus.Incr("mqtt-relay-consumer", 1)
		relayCh <- &types.RelayMessage{
			Value:   msg,
			Options: &types.RelayMessageOptions{},
		}
	}

	token := m.client.Subscribe(relayOpts.Mqtt.Args.Topic, byte(m.connArgs.QosLevel), readFunc)
	if err := token.Error(); err != nil {
		return err
	}

	<-ctx.Done()
	m.log.Debug("Received shutdown signal, exiting relayer")

	return nil
}

func validateRelayOptions(relayOpts *opts.RelayOptions) error {
	if relayOpts == nil {
		return validate.ErrEmptyRelayOpts
	}

	if relayOpts.Mqtt == nil {
		return validate.ErrEmptyBackendGroup
	}

	if relayOpts.Mqtt.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if relayOpts.Mqtt.Args.Topic == "" {
		return ErrEmptyTopic
	}

	return nil
}
