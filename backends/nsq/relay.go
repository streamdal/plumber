package nsq

import (
	"context"

	"github.com/nsqio/go-nsq"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/backends/nsq/types"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
	"github.com/batchcorp/plumber/stats"
)

type Relayer struct {
	Options         *cli.Options
	RelayCh         chan interface{}
	log             *logrus.Entry
	ShutdownContext context.Context
}

func Relay(opts *cli.Options, relayCh chan interface{}, shutdownCtx context.Context) (relay.IRelayBackend, error) {
	if err := validateRelayOptions(opts); err != nil {
		return nil, errors.Wrap(err, "unable to verify options")
	}

	return &Relayer{
		Options:         opts,
		RelayCh:         relayCh,
		log:             logrus.WithField("pkg", "nsq/relay"),
		ShutdownContext: shutdownCtx,
	}, nil
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

	for {
		select {
		case <-r.ShutdownContext.Done():
			r.log.Info("Received shutdown signal, existing relayer")
			return nil
		default:
			// noop
		}
	}

	return nil
}
