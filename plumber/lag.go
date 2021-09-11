package plumber

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
)

// TODO: Move this into kafka

// HandleLagCmd handles viewing lag in CLI mode
func (p *Plumber) HandleLagCmd() error {
	if p.KongCtx != "lag kafka" {
		return errors.New("fetching consumer lag is only supported with kafka backend")
	}

	backendName, err := util.GetBackendName(p.KongCtx)
	if err != nil {
		return errors.Wrap(err, "unable to get backend")
	}

	backend, err := backends.New(backendName, p.CLIOptions)
	if err != nil {
		return errors.Wrap(err, "unable to instantiate backend")
	}

	resultsCh := make(chan []*types.TopicStats, 100)

	go func() {
		if err := backend.Lag(p.ServiceShutdownCtx, resultsCh, 5*time.Second); err != nil {
			p.log.Errorf("unable to get lag stats: %s", err)
			close(resultsCh)
		}
	}()

	for stats := range resultsCh {
		displayLag(stats, time.Now().UTC())
	}

	p.log.Debug("lag exiting")

	return nil
}

func displayLag(stats []*types.TopicStats, t time.Time) {
	properties := make([][]string, 0)

	for _, v := range stats {
		if len(properties) != 0 {
			// Add a separator between diff topics
			properties = append(properties, []string{
				"---------------------------------------------------------------",
			})
		}

		properties = append(properties, []string{
			"Topic", v.TopicName,
		}, []string{
			"Group ID", v.GroupID,
		})

		for partitionID, pstats := range v.Partitions {
			properties = append(properties, []string{
				"Partition ID", fmt.Sprint(partitionID),
			}, []string{
				"  └─ Messages Behind", fmt.Sprint(pstats.MessagesBehind),
			})
		}
	}

	printer.PrintTableProperties(properties, t)
}
