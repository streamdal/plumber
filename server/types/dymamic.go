package types

import (
	"context"
	"fmt"
	"time"

	"github.com/batchcorp/plumber/util"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/dynamic"

	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/backends"
)

type Dynamic struct {
	Active         bool                 `json:"-"`
	Id             string               `json:"-"`
	CancelCtx      context.Context      `json:"-"`
	CancelFunc     context.CancelFunc   `json:"-"`
	Backend        backends.Backend     `json:"-"`
	Options        *opts.DynamicOptions `json:"config"`
	DynamicService dynamic.IDynamic

	log *logrus.Entry
}

func (d *Dynamic) StartDynamic(delay time.Duration) error {
	d.log = logrus.WithField("pkg", "types/dynamic")

	// Start up dynamic connection
	dynamicSvc, err := dynamic.New(d.Options, d.Backend.Name())
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}
	d.DynamicService = dynamicSvc

	localErrCh := make(chan *records.ErrorRecord, 1)

	d.Active = true
	d.Options.XActive = true

	go func() {
		d.log.Info("Starting dynamic replay")

		if err := d.Backend.Dynamic(d.CancelCtx, d.Options, dynamicSvc); err != nil {
			util.WriteError(d.log, localErrCh, fmt.Errorf("error during dynamic replay (id: %s): %s", d.Id, err))

			// Cancel worker
			d.CancelFunc()

			// Give it a sec
			time.Sleep(time.Second)

			// Clean up connection to user's message bus
			d.Close()
		}
	}()

	timeAfterCh := time.After(delay)

	// Will block for =< delay
	select {
	case <-timeAfterCh:
		d.log.Debugf("dynamic replay id '%s' success after %s wait", d.Id, delay.String())
		break
	case err := <-localErrCh:
		return fmt.Errorf("relay startup failed for id '%s': %s", d.Id, err.Error)
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
