package types

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/util"

	"github.com/batchcorp/plumber/relay"
)

type Relay struct {
	Active     bool               `json:"-"`
	Id         string             `json:"-"`
	CancelCtx  context.Context    `json:"-"`
	CancelFunc context.CancelFunc `json:"-"`
	RelayCh    chan interface{}   `json:"-"`
	Backend    backends.Backend   `json:"-"`
	Options    *opts.RelayOptions `json:"config"`

	log *logrus.Entry
}

// StartRelay starts a configured relay, it's workers, and the GRPC workers
func (r *Relay) StartRelay(delay time.Duration) error {
	r.log = logrus.WithField("pkg", "types/relay")

	relayCh := make(chan interface{}, 1)
	localErrCh := make(chan *records.ErrorRecord, 1)

	// Needed to satisfy relay.Options{}, not used
	_, stubCancelFunc := context.WithCancel(context.Background())

	relayCfg := &relay.Config{
		Token:              r.Options.CollectionToken,
		GRPCAddress:        r.Options.XBatchshGrpcAddress,
		NumWorkers:         5,
		Timeout:            util.DurationSec(r.Options.XBatchshGrpcTimeoutSeconds),
		RelayCh:            relayCh,
		DisableTLS:         r.Options.XBatchshGrpcDisableTls,
		BatchSize:          r.Options.BatchSize,
		Type:               r.Backend.Name(),
		MainShutdownFunc:   stubCancelFunc,
		ServiceShutdownCtx: r.CancelCtx,
	}

	grpcRelayer, err := relay.New(relayCfg)
	if err != nil {
		return errors.Wrap(err, "unable to create new relayer instance")
	}

	// Launch gRPC Workers
	if err := grpcRelayer.StartWorkers(r.CancelCtx); err != nil {
		return errors.Wrap(err, "unable to start gRPC relay workers")
	}

	go func() {
		// TODO: Need to update relayer to use error channel
		if err := r.Backend.Relay(r.CancelCtx, r.Options, relayCh, nil); err != nil {
			util.WriteError(r.log, localErrCh, fmt.Errorf("error during relay (id: %s): %s", r.Id, err))
			r.CancelFunc()
		}

		r.log.Debugf("relay id '%s' exiting", r.Id)
	}()

	timeAfterCh := time.After(delay)

	// Will block for =< delay
	select {
	case <-timeAfterCh:
		r.log.Debugf("relay id '%s' success after %s wait", r.Id, delay.String())
		break
	case err := <-localErrCh:
		return fmt.Errorf("relay startup failed for id '%s': %s", r.Id, err.Error)
	}

	return nil
}
