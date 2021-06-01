package mqtt

import (
	"context"
	"fmt"
	"strings"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/backends/mqtt/types"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
	"github.com/batchcorp/plumber/stats"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"
)

type Relayer struct {
	Client  pahomqtt.Client
	Options *cli.Options
	MsgDesc *desc.MessageDescriptor
	RelayCh chan interface{}
	log     *logrus.Entry
	Looper  *director.FreeLooper
	Context context.Context
}

type IMQTTRelayer interface {
	Relay() error
}

// Relay sets up a new MQTT relayer, starts GRPC workers and the API server
func Relay(opts *cli.Options) error {
	if err := validateRelayOptions(opts); err != nil {
		return errors.Wrap(err, "unable to verify options")
	}

	// Create new relayer instance (+ validate token & gRPC address)
	relayCfg := &relay.Config{
		Token:       opts.RelayToken,
		GRPCAddress: opts.RelayGRPCAddress,
		NumWorkers:  opts.RelayNumWorkers,
		Timeout:     opts.RelayGRPCTimeout,
		RelayCh:     make(chan interface{}, 1),
		DisableTLS:  opts.RelayGRPCDisableTLS,
		Type:        opts.RelayType,
	}

	grpcRelayer, err := relay.New(relayCfg)
	if err != nil {
		return errors.Wrap(err, "unable to create new gRPC relayer")
	}

	// Launch HTTP server
	go func() {
		if err := api.Start(opts.RelayHTTPListenAddress, opts.Version); err != nil {
			logrus.Fatalf("unable to start API server: %s", err)
		}
	}()

	if err := grpcRelayer.StartWorkers(); err != nil {
		return errors.Wrap(err, "unable to start gRPC relay workers")
	}

	client, err := connect(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create client")
	}

	r := &Relayer{
		Client:  client,
		Options: opts,
		RelayCh: relayCfg.RelayCh,
		log:     logrus.WithField("pkg", "mqtt/relay"),
		Looper:  director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
		Context: context.Background(),
	}

	return r.Relay()
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
		case <-r.Context.Done():
			return nil
		}
	}

	return nil
}
