package nsq

import (
	"context"
	"sync"

	"github.com/jhump/protoreflect/desc"
	"github.com/nsqio/go-nsq"
	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/backends/nsq/types"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
	"github.com/batchcorp/plumber/stats"
)

type Relayer struct {
	Options        *cli.Options
	MsgDesc        *desc.MessageDescriptor
	RelayCh        chan interface{}
	log            *logrus.Entry
	Looper         *director.FreeLooper
	DefaultContext context.Context
}

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

	r := &Relayer{
		Options:        opts,
		RelayCh:        relayCfg.RelayCh,
		log:            logrus.WithField("pkg", "nsq/relay"),
		Looper:         director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
		DefaultContext: context.Background(),
	}

	return r.Relay()
}

func validateRelayOptions(opts *cli.Options) error {
	// These currently accept the same params
	return validateReadOptions(opts)
}

// Relay reads messages from NSQ and sends them to RelayCh which is then read by relay.Run()
func (r *Relayer) Relay() error {
	r.log.Infof("Relaying NSQ messages from topic '%s', channel '%s' -> %s",
		r.Options.NSQ.Topic, r.Options.NSQ.Channel, r.Options.RelayGRPCAddress)

	r.log.Infof("HTTP server listening on '%s'", r.Options.RelayHTTPListenAddress)

	config, err := getNSQConfig(r.Options)
	if err != nil {
		return errors.Wrap(err, "unable to create NSQ config")
	}

	consumer, err := nsq.NewConsumer(r.Options.NSQ.Topic, r.Options.NSQ.Channel, config)
	if err != nil {
		return errors.Wrap(err, "Could not start NSQ consumer")
	}

	// Use logrus for NSQ logs
	consumer.SetLogger(nil, nsq.LogLevelError)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	consumer.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
		stats.Incr("nsq-relay-consumer", 1)

		r.log.Debugf("Writing NSQ message to relay channel: %s", string(msg.Body))

		r.RelayCh <- &types.RelayMessage{
			Value: msg,
			Options: &types.RelayMessageOptions{
				Topic:   r.Options.NSQ.Topic,
				Channel: r.Options.NSQ.Channel,
			},
		}

		return nil
	}))

	// Connect to correct server. Reading can be done directly from an NSQD server
	// or let lookupd find the correct one.
	if r.Options.NSQ.NSQLookupDAddress != "" {
		if err := consumer.ConnectToNSQLookupd(r.Options.NSQ.NSQLookupDAddress); err != nil {
			return errors.Wrap(err, "could not connect to nsqlookupd")
		}
	} else {
		if err := consumer.ConnectToNSQD(r.Options.NSQ.NSQDAddress); err != nil {
			return errors.Wrap(err, "could not connect to nsqd")
		}
	}

	defer consumer.Stop()

	wg.Wait()

	return nil
}
