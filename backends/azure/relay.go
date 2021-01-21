package azure

import (
	"context"

	servicebus "github.com/Azure/azure-service-bus-go"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/backends/azure/types"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
)

type Relayer struct {
	Options        *cli.Options
	MsgDesc        *desc.MessageDescriptor
	RelayCh        chan interface{}
	log            *logrus.Entry
	Service        *servicebus.Queue
	QueueName      string
	DefaultContext context.Context
}

func Relay(opts *cli.Options) error {

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

	// Create new service
	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create new azure service")
	}

	queue, err := client.NewQueue(opts.Azure.Queue)
	if err != nil {
		return err
	}

	// Launch HTTP server
	go func() {
		if err := api.Start(opts.RelayHTTPListenAddress, opts.Version); err != nil {
			logrus.Fatalf("unable to start API server: %s", err)
		}
	}()

	// Launch gRPC Relayer
	if err := grpcRelayer.StartWorkers(); err != nil {
		return errors.Wrap(err, "unable to start gRPC relay workers")
	}

	r := &Relayer{
		Service:   queue,
		QueueName: opts.Azure.Queue,
		Options:   opts,
		RelayCh:   relayCfg.RelayCh,
		log:       logrus.WithField("pkg", "azure/relay.go"),
	}

	return r.Relay()
}

func (r *Relayer) Relay() error {
	ctx := context.Background()
	defer r.Service.Close(ctx)
	r.log.Infof("Relaying azure service bus messages from '%s' queue -> '%s'",
		r.Options.Azure.Queue, r.Options.RelayGRPCAddress)

	r.log.Infof("HTTP server listening on '%s'", r.Options.RelayHTTPListenAddress)

	var handler servicebus.HandlerFunc = func(ctx context.Context, msg *servicebus.Message) error {
		r.log.Debug("Writing message to relay channel")

		r.RelayCh <- &types.RelayMessage{
			Value: msg,
		}

		defer msg.Complete(ctx)
		return nil
	}

	for {
		if err := r.Service.ReceiveOne(ctx, handler); err != nil {
			return err
		}
	}

	return nil
}
