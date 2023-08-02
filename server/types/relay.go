package types

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/relay"
	"github.com/batchcorp/plumber/util"
)

type Relay struct {
	Active     bool               `json:"-"`
	Id         string             `json:"-"`
	CancelCtx  context.Context    `json:"-"`
	CancelFunc context.CancelFunc `json:"-"`
	Backend    backends.Backend   `json:"-"`
	Options    *opts.RelayOptions `json:"config"`

	log *logrus.Entry
}

// StartRelay starts a configured relay, it's workers, and the GRPC workers;
// StartRelay will block for "delay" duration, waiting for errors.
func (r *Relay) StartRelay(delay time.Duration) error {
	r.log = logrus.WithField("pkg", "types/relay")

	relayCh := make(chan interface{}, 1)
	localErrCh := make(chan *records.ErrorRecord, 1)

	// Needed to satisfy relay.Options{}, not used
	stubMainCtx, stubCancelFunc := context.WithCancel(context.Background())

	relayCfg := &relay.Config{
		Token:              r.Options.CollectionToken,
		GRPCAddress:        r.Options.XStreamdalGrpcAddress,
		NumWorkers:         5,
		Timeout:            util.DurationSec(r.Options.XStreamdalGrpcTimeoutSeconds),
		RelayCh:            relayCh,
		DisableTLS:         r.Options.XStreamdalGrpcDisableTls,
		BatchSize:          r.Options.BatchSize,
		Type:               r.Backend.Name(),
		ServiceShutdownCtx: r.CancelCtx,
		MainShutdownCtx:    stubMainCtx,    // Needed to satisfy relay.Options{}, not used in server mode
		MainShutdownFunc:   stubCancelFunc, // Needed to satisfy relay.Options{}, not used in server mode
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

// MarshalJSON marshals a tunnel to JSON
func (r *Relay) MarshalJSON() ([]byte, error) {
	buf := bytes.NewBuffer([]byte(``))

	m := jsonpb.Marshaler{}
	if err := m.Marshal(buf, r.Options); err != nil {
		return nil, errors.Wrap(err, "could not marshal opts.RelayOptions")
	}

	return buf.Bytes(), nil
}

// UnmarshalJSON unmarshals JSON into a tunnel struct
func (r *Relay) UnmarshalJSON(v []byte) error {
	relay := &opts.RelayOptions{}
	if err := jsonpb.Unmarshal(bytes.NewBuffer(v), relay); err != nil {
		return errors.Wrap(err, "unable to unmarshal stored relay")
	}

	r.Options = relay
	r.Id = relay.XRelayId
	r.Active = relay.XActive

	return nil
}
