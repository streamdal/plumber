package mqtt

import (
	"context"
	"fmt"
	"strings"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/backends/mqtt/types"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
	"github.com/batchcorp/plumber/stats"
)

type Relayer struct {
	Client      pahomqtt.Client
	Options     *cli.Options
	RelayCh     chan interface{}
	log         *logrus.Entry
	Looper      *director.FreeLooper
	ShutdownCtx context.Context
}

// Relay sets up a new MQTT relayer
func Relay(opts *cli.Options, relayCh chan interface{}, shutdownCtx context.Context) (relay.IRelayBackend, error) {
	if err := validateRelayOptions(opts); err != nil {
		return nil, errors.Wrap(err, "unable to verify options")
	}

	client, err := connect(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create client")
	}

	return &Relayer{
		Client:      client,
		Options:     opts,
		RelayCh:     relayCh,
		log:         logrus.WithField("pkg", "mqtt/relay"),
		Looper:      director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
		ShutdownCtx: shutdownCtx,
	}, nil
}

// validateRelayOptions ensures all required CLI options are present before initializing relay mode
func validateRelayOptions(opts *cli.Options) error {
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

	// If anything protobuf-related is specified, it's being used
	if opts.ReadProtobufRootMessage != "" || len(opts.ReadProtobufDirs) != 0 {
		if err := cli.ValidateProtobufOptions(
			opts.ReadProtobufDirs,
			opts.ReadProtobufRootMessage,
		); err != nil {
			return fmt.Errorf("unable to validate protobuf option(s): %s", err)
		}
	}

	return nil
}

// Relay reads messages from MQTT and sends them to RelayCh which is then read by relay.Run()
func (r *Relayer) Relay() error {
	r.log.Infof("Relaying MQTT messages from topic '%s' -> '%s'",
		r.Options.MQTT.Topic, r.Options.RelayGRPCAddress)

	r.log.Infof("HTTP server listening on '%s'", r.Options.RelayHTTPListenAddress)

	defer r.Client.Disconnect(0)

	r.Client.Subscribe(r.Options.MQTT.Topic, byte(r.Options.MQTT.QoSLevel), func(client mqtt.Client, msg mqtt.Message) {

		stats.Incr("mqtt-relay-consumer", 1)

		// Generate relay message
		r.RelayCh <- &types.RelayMessage{
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

	return nil
}
