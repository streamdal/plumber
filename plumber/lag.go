package plumber

import (
	"context"
	"time"

	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
)

// HandleLagCmd handles viewing lag in CLI mode
func (p *Plumber) HandleLagCmd() error {
	if p.Cmd != "lag kafka" {
		return errors.New("fetching consumer lag is only supported with kafka backend")
	}

	backendName, err := util.GetBackendName(p.Cmd)
	if err != nil {
		return errors.Wrap(err, "unable to get backend")
	}

	backend, err := backends.New(backendName, p.Options)
	if err != nil {
		return errors.Wrap(err, "unable to instantiate backend")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t := time.NewTicker(5 * time.Second)

	for range t.C {
		stats, err := backend.Lag(ctx)
		if err != nil {
			p.log.Errorf("unable to determine consumer lag: %s", err)
			continue
		}

		displayLag(stats)
	}

	return nil
}

// TODO: Implement
func displayLag(stats *types.LagStats) {

}
