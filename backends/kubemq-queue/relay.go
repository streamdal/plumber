package kubemq_queue

import (
	"context"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"
)

type Relayer struct {
	Options     *cli.Options
	RelayCh     chan interface{}
	log         *logrus.Entry
	Looper      *director.FreeLooper
	ShutdownCtx context.Context
}

func Relay(opts *cli.Options, relayCh chan interface{}, shutdownCtx context.Context) (relay.IRelayBackend, error) {
	if err := validateRelayOptions(opts); err != nil {
		return nil, errors.Wrap(err, "unable to verify options")
	}

	return &Relayer{
		Options:     opts,
		RelayCh:     relayCh,
		log:         logrus.WithField("pkg", "rabbitmq/relay"),
		Looper:      director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
		ShutdownCtx: shutdownCtx,
	}, nil
}

func validateRelayOptions(opts *cli.Options) error {
	if opts.KubeMQQueue.Queue == "" {
		return errors.New("You must specify a routing key")
	}

	return nil
}

func (r *Relayer) Relay() error {

	r.log.Infof("Relaying KubeMQ messages from '%s' -> '%s'",
		r.Options.KubeMQQueue.Queue, r.Options.RelayGRPCAddress)

	r.log.Infof("HTTP server listening on '%s'", r.Options.RelayHTTPListenAddress)

	return nil
}
