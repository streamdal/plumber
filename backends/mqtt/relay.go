package mqtt

import (
	"context"

	"github.com/batchcorp/plumber/prometheus"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends/mqtt/types"
)

func (m *MQTT) Relay(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}, errorCh chan *records.ErrorRecord) error {
	if err := validateRelayOptions(relayOpts); err != nil {
		return errors.Wrap(err, "unable to validate relay options")
	}

	var readFunc = func(client mqtt.Client, msg mqtt.Message) {
		prometheus.Incr("mqtt-relay-consumer", 1)
		relayCh <- &types.RelayMessage{
			Value: msg,
		}
	}

	token := m.client.Subscribe(relayOpts.Mqtt.Args.Topic, byte(m.connArgs.QosLevel), readFunc)
	if err := token.Error(); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			m.log.Info("Received shutdown signal, existing relayer")
			return nil
		default:
			// noop
		}
	}

	return nil
}

func validateRelayOptions(relayOpts *opts.RelayOptions) error {
	if relayOpts == nil {
		return errors.New("relay options cannot be nil")
	}

	if relayOpts.Mqtt == nil {
		return errors.New("backend group options cannot be nil")
	}

	if relayOpts.Mqtt.Args == nil {
		return errors.New("backend arg options cannot be nil")
	}

	if relayOpts.Mqtt.Args.Topic == "" {
		return errors.New("topic cannot be empty")
	}

	return nil
}
