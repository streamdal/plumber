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
	"github.com/batchcorp/plumber/tunnel"
	"github.com/batchcorp/plumber/util"
)

type Tunnel struct {
	Active           bool                 `json:"-"`
	Id               string               `json:"-"`
	CancelCtx        context.Context      `json:"-"`
	CancelFunc       context.CancelFunc   `json:"-"`
	Backend          backends.Backend     `json:"-"`
	Options          *opts.DynamicOptions `json:"config"`
	TunnelService    tunnel.ITunnel
	PlumberClusterID string `json:"-"`

	log *logrus.Entry
}

// StartTunnel will attempt to start the replay tunnel. Upon the start, it will
// wait for the given "delay" listening for errors. It will return an error
// if it encounters any errors on the ErrorChan or if the Tunnel call fails.
//
// Subsequent failures inside of Tunnel() are not handled yet.
func (d *Tunnel) StartTunnel(delay time.Duration) error {
	d.log = logrus.WithField("pkg", "types/tunnel")

	d.log.Debugf("Plumber cluster ID: %s", d.PlumberClusterID)

	// Create a new dynamic connection
	dynamicSvc, err := tunnel.New(d.Options, d.PlumberClusterID)
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	d.TunnelService = dynamicSvc

	localErrCh := make(chan *records.ErrorRecord, 1)

	d.Active = true
	d.Options.XActive = true

	go func() {
		// Blocks until dynamic is closed
		if err := d.Backend.Tunnel(d.CancelCtx, d.Options, dynamicSvc, localErrCh); err != nil {
			util.WriteError(d.log, localErrCh, fmt.Errorf("error during tunnel (id: %s): %s", d.Id, err))

			// Cancel any goroutines spawned by Tunnel()
			d.CancelFunc()

			// Give it a sec
			time.Sleep(time.Second)

			// Clean up connection to user's message bus
			d.Close()
		}

		d.log.Debugf("goroutine exiting for tunnel_id '%s'", d.Id)
	}()

	timeAfterCh := time.After(delay)

	// Will block for =< delay
	select {
	case <-timeAfterCh:
		d.log.Debugf("tunnel id '%s' success after %s wait", d.Id, delay.String())
		break
	case err := <-localErrCh:
		return fmt.Errorf("tunnel startup failed for id '%s': %s", d.Id, err.Error)
	}

	return nil

}

func (d *Tunnel) Close() {
	// Clean up connection to user's message bus
	if err := d.Backend.Close(context.Background()); err != nil {
		d.log.Errorf("error closing tunnel backend: %s", err)
	}

	// Clean up gRPC connection
	if err := d.TunnelService.Close(); err != nil {
		d.log.Errorf("error closing tunnel gRPC connection: %s", err)
	}

	// This gets re-set on ResumeTunnel
	d.Backend = nil
}

// MarshalJSON marshals a tunnel to JSON
func (d *Tunnel) MarshalJSON() ([]byte, error) {
	buf := bytes.NewBuffer([]byte(``))

	m := jsonpb.Marshaler{}
	if err := m.Marshal(buf, d.Options); err != nil {
		return nil, errors.Wrap(err, "could not marshal opts.DynamicOptions")
	}

	return buf.Bytes(), nil
}

// UnmarshalJSON unmarshals JSON into a tunnel struct
func (d *Tunnel) UnmarshalJSON(v []byte) error {
	dynamic := &opts.DynamicOptions{}
	if err := jsonpb.Unmarshal(bytes.NewBuffer(v), dynamic); err != nil {
		return errors.Wrap(err, "unable to unmarshal stored tunnel")
	}

	d.Options = dynamic
	d.Id = dynamic.XDynamicId
	d.Active = dynamic.XActive

	return nil
}
