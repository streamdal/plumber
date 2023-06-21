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
		return errors.Wrap(err, "counter not found")
	}

	vc.Add(counter.Value)

	a.log.Debugf("increased %s counter by %.2f", counter.Name, counter.Value)

	return nil
}

func getKeys(m map[string]string) []string {
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	return keys
}
