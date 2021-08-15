package plumber

import (
	"context"
	"fmt"
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

	resultsCh := make(chan []*types.TopicStats, 100)

	go func() {
		if err := backend.Lag(ctx, resultsCh, 5*time.Second); err != nil {
			p.log.Errorf("unable to get lag stats: %s", err)
			close(resultsCh)
		}
	}()

	for stats := range resultsCh {
		displayLag(stats)
	}

	p.log.Debug("lag exiting")

	return nil
}

// TODO: Improve
func displayLag(stats []*types.TopicStats) {
	for _, v := range stats {
		for partitionID, pstats := range v.Partitions {
			fmt.Printf("Lag in partition %v is <%v messages> for consumer with group-id '%s'\n",
				partitionID, pstats.MessagesBehind, v.GroupID)
		}
	}
}
