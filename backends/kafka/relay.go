package kafka

import (
	"context"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/backends/kafka/types"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
)

type Relayer struct {
	Options        *cli.Options
	MsgDesc        *desc.MessageDescriptor
	RelayCh        chan interface{}
	log            *logrus.Entry
	Looper         *director.FreeLooper
	DefaultContext context.Context
}

type IKafkaRelayer interface {
	Relay() error
}

var (
	errMissingTopic = errors.New("You must specify a topic")
)

// Relay sets up a new Kafka relayer, starts GRPC workers and the API server
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

	r := &Relayer{
		Options:        opts,
		RelayCh:        relayCfg.RelayCh,
		log:            logrus.WithField("pkg", "kafka/relay"),
		Looper:         director.NewFreeLooper(director.FOREVER, make(chan error)),
		DefaultContext: context.Background(),
	}

	return r.Relay()
}

// validateRelayOptions ensures all required CLI options are present before initializing relay mode
func validateRelayOptions(opts *cli.Options) error {
	if opts.Kafka.Topic == "" {
		return errMissingTopic
	}
	return nil
}

// Relay reads messages from Kafka and sends them to RelayCh which is then read by relay.Run()
func (r *Relayer) Relay() error {
	r.log.Infof("Relaying Kafka messages from '%s' topic -> '%s'",
		r.Options.Kafka.Topic, r.Options.RelayGRPCAddress)

	r.log.Infof("HTTP server listening on '%s'", r.Options.RelayHTTPListenAddress)

	reader, err := NewReader(r.Options)
	if err != nil {
		return err
	}
	defer reader.Reader.Close()
	defer reader.Conn.Close()

	for {
		msg, err := reader.Reader.ReadMessage(r.DefaultContext)
		if err != nil {
			r.log.Errorf("Unable to read message: %s", err)
			continue
		}
		r.log.Infof("Writing Kafka message to relay channel: %s", msg.Value)
		r.RelayCh <- &types.RelayMessage{
			Value:   &msg,
			Options: &types.RelayMessageOptions{},
		}
	}
}
