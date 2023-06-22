package actions

import (
	"context"

	"github.com/pkg/errors"

	counters "github.com/batchcorp/plumber/prometheus"
	"github.com/batchcorp/plumber/server/types"
)

func (a *Actions) Counter(_ context.Context, counter *types.Counter) error {
	// Increase local counter, dumper goroutine will pick it up and send to prometheus
	c := counters.GetVecCounter(counter.Subsystem, counter.Name)
	if c == nil {
		return errors.Errorf("counter not found: %s", counter.Name)
	}
	vc, err := c.GetMetricWith(counter.Labels)
	if err != nil {
		return errors.Wrapf(err, "counter not found: %#v", counter)
	}

	vc.Add(counter.Value)

	a.log.Debugf("increased %s counter by %.2f", counter.Name, counter.Value)

	return nil
}
