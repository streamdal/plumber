package mqtt

import (
	"context"
	"strings"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"

	mtypes "github.com/batchcorp/plumber/backends/mqtt/types"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/stats"
	"github.com/batchcorp/plumber/types"
)

type Relayer struct {
	Client      pahomqtt.Client
	Options     *options.Options
	RelayCh     chan interface{}
	ErrorCh     chan *types.ErrorMessage
	log         *logrus.Entry
	Looper      *director.FreeLooper
	ShutdownCtx context.Context
}

// Relay sets up a new MQTT relayer
func (m *MQTT) Relay(ctx context.Context, relayCh chan interface{}, errorCh chan *types.ErrorMessage) error {
	if err := validateRelayOptions(m.Options); err != nil {
		return errors.Wrap(err, "unable to verify options")
	}

	r := &Relayer{
		Client:      m.client,
		Options:     m.Options,
		RelayCh:     relayCh,
		ErrorCh:     errorCh,
		log:         logrus.WithField("pkg", "mqtt/relay"),
		Looper:      director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
		ShutdownCtx: ctx,
	}

	return r.Relay()
}

// Relay reads messages from MQTT and sends them to RelayCh which is then read by relay.Run()
func (r *Relayer) Relay() error {
	r.log.Infof("Relaying MQTT messages from topic '%s' -> '%s'",
		r.Options.MQTT.Topic, r.Options.Relay.GRPCAddress)

	r.log.Infof("HTTP server listening on '%s'", r.Options.Relay.HTTPListenAddress)

	r.Client.Subscribe(r.Options.MQTT.Topic, byte(r.Options.MQTT.QoSLevel), func(client mqtt.Client, msg mqtt.Message) {
		stats.Incr("mqtt-relay-consumer", 1)

		// Generate relay message
		r.RelayCh <- &mtypes.RelayMessage{
			Value: msg,
		}

		r.log.Debugf("Successfully relayed message '%d' from topic '%s'", msg.MessageID(), msg.Topic())

	})

	for {
		select {
		case <-r.ShutdownCtx.Done():
			r.log.Info("Received shutdown signal, existing relayer")
			return nil
		default:
			// noop
		}
	}
}

// validateRelayOptions ensures all required CLI options are present before initializing relay mode
func validateRelayOptions(opts *options.Options) error {
	if opts.MQTT.Address == "" {
		return errMissingAddress
	}

	if opts.MQTT.Topic == "" {
		return errMissingTopic
	}

	if strings.HasPrefix(opts.MQTT.Address, "ssl") {
		if opts.MQTT.TLSClientKeyFile == "" {
			return errMissingTLSKey
		}

		if opts.MQTT.TLSClientCertFile == "" {
			return errMissingTlsCert
		}

		if opts.MQTT.TLSCAFile == "" {
			return errMissingTLSCA
		}
	}

	if opts.MQTT.QoSLevel > 2 || opts.MQTT.QoSLevel < 0 {
		return errInvalidQOSLevel
	}

	return nil
}
