package actions

import (
	"context"

	"github.com/batchcorp/plumber/prometheus"
	"github.com/batchcorp/plumber/server/types"
)

func (a *Actions) Counter(ctx context.Context, counter *types.Counter) error {
	// Increase local counter, dumper goroutine will pick it up and send to prometheus
	prometheus.IncrPromCounter(counter.Type, counter.Value)
	a.log.Debugf("increased %s counter by %.2f", counter.Type, counter.Value)
	return nil
}
