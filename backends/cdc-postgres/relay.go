package cdc_postgres

import (
	"context"
	"github.com/batchcorp/plumber/api"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Relayer struct {
	Options        *cli.Options
	RelayCh        chan interface{}
	log            *logrus.Entry
	Service        *pgx.ReplicationConn
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
		Type:        opts.RelayType,
	}

	grpcRelayer, err := relay.New(relayCfg)
	if err != nil {
		return errors.Wrap(err, "unable to create new gRPC relayer")
	}

	// Create new service
	client, err := NewService(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create postgres connection")
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
		Options: opts,
		RelayCh: relayCfg.RelayCh,
		Service: client,
		log:     logrus.WithField("pkg", "azure/relay.go"),
	}

	return r.Relay()
}

func (r *Relayer) Relay() error {
	return nil
}
