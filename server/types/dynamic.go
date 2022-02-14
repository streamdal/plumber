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
	"github.com/batchcorp/plumber/dynamic"
	"github.com/batchcorp/plumber/util"
)

type Dynamic struct {
	Active           bool                 `json:"-"`
	Id               string               `json:"-"`
	CancelCtx        context.Context      `json:"-"`
	CancelFunc       context.CancelFunc   `json:"-"`
	Backend          backends.Backend     `json:"-"`
	Options          *opts.DynamicOptions `json:"config"`
	DynamicService   dynamic.IDynamic
	PlumberClusterID string `json:"-"`

	log *logrus.Entry
}

// StartDynamic will attempt to start the replay tunnel. Upon the start, it will
// wait for the given "delay" listening for errors. It will return an error
// if it encounters any errors on the ErrorChan or if the Dynamic call fails.
//
// Subsequent failures inside of Dynamic() are not handled yet.
func (d *Dynamic) StartDynamic(delay time.Duration) error {
	d.log = logrus.WithField("pkg", "types/dynamic")

	d.log.Debugf("Plumber cluster ID: %s", d.PlumberClusterID)

	// Create a new dynamic connection
	dynamicSvc, err := dynamic.New(d.Options, d.PlumberClusterID)
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	d.DynamicService = dynamicSvc

	localErrCh := make(chan *records.ErrorRecord, 1)

	d.Active = true
	d.Options.XActive = true

	go func() {
		// Blocks until dynamic is closed
		if err := d.Backend.Dynamic(d.CancelCtx, d.Options, dynamicSvc, localErrCh); err != nil {
			util.WriteError(d.log, localErrCh, fmt.Errorf("error during dynamic replay (id: %s): %s", d.Id, err))

			// Cancel any goroutines spawned by Dynamic()
			d.CancelFunc()

			// Give it a sec
			time.Sleep(time.Second)

			// Clean up connection to user's message bus
			d.Close()
		}

		d.log.Debugf("goroutine exiting for dynamic_id '%s'", d.Id)
	}()

	timeAfterCh := time.After(delay)

	// Will block for =< delay
	select {
	case <-timeAfterCh:
		d.log.Debugf("dynamic replay id '%s' success after %s wait", d.Id, delay.String())
		break
	case err := <-localErrCh:
		return fmt.Errorf("dynamic startup failed for id '%s': %s", d.Id, err.Error)
	}

	return nil

}

func (d *Dynamic) Close() {
	// Clean up connection to user's message bus
	if err := d.Backend.Close(context.Background()); err != nil {
		d.log.Errorf("error closing dynamic replay backend: %s", err)
	}

	// Clean up gRPC connection
	if err := d.DynamicService.Close(); err != nil {
		d.log.Errorf("error closing dynamic replay gRPC connection: %s", err)
	}

	// This gets re-set on ResumeDynamic
	d.Backend = nil
}

// MarshalJSON marshals a dynamic replay to JSON
func (d *Dynamic) MarshalJSON() ([]byte, error) {
	buf := bytes.NewBuffer([]byte(``))

	m := jsonpb.Marshaler{}
	if err := m.Marshal(buf, d.Options); err != nil {
		return nil, errors.Wrap(err, "could not marshal opts.DynamicOptions")
	}

	return buf.Bytes(), nil
}

// UnmarshalJSON unmarshals JSON into a dynamic replay struct
func (d *Dynamic) UnmarshalJSON(v []byte) error {
	dynamic := &opts.DynamicOptions{}
	if err := jsonpb.Unmarshal(bytes.NewBuffer(v), dynamic); err != nil {
		return errors.Wrap(err, "unable to unmarshal stored dynamic replay")
	}

	d.Options = dynamic
	d.Id = dynamic.XDynamicId
	d.Active = dynamic.XActive

	return nil
}
