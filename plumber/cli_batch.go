package plumber

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/posthog/posthog-go"

	"github.com/batchcorp/plumber/backends/batch"
	"github.com/batchcorp/plumber/options"
)

// HandleBatchCmd handles all commands related to Batch.sh API
func (p *Plumber) HandleBatchCmd() error {
	b := batch.New(p.CLIOptions, p.PersistentConfig)

	// Less typing
	cmd := p.CLIOptions.Global.XFullCommand

	p.logMetricBatch(cmd)

	// Handle the command
	switch {
	case strings.HasPrefix(cmd, "batch login"):
		return b.Login()
	case strings.HasPrefix(cmd, "batch logout"):
		return b.Logout()
	case strings.HasPrefix(cmd, "batch list collection"):
		return b.ListCollections()
	case strings.HasPrefix(cmd, "batch create collection"):
		return b.CreateCollection()
	case strings.HasPrefix(cmd, "batch create destination"):
		commands := strings.Split(cmd, " ")
		return b.CreateDestination(commands[3])
	case strings.HasPrefix(cmd, "batch list destination"):
		return b.ListDestinations()
	case strings.HasPrefix(cmd, "batch list schema"):
		return b.ListSchemas()
	case strings.HasPrefix(cmd, "batch list replay"):
		return b.ListReplays()
	case strings.HasPrefix(cmd, "batch create replay"):
		return b.CreateReplay()
	case strings.HasPrefix(cmd, "batch archive replay"):
		return b.ArchiveReplay()
	case strings.HasPrefix(cmd, "batch search"):
		return b.SearchCollection()
	default:
		return fmt.Errorf("unrecognized command: %s", p.CLIOptions.Global.XFullCommand)
	}
}

// logMetricBatch sends a metric with only the command name.
// We don't want to ingest flags for privacy
func (p *Plumber) logMetricBatch(cmd string) {
	var cmdName string

	parts := strings.Split(cmd, " ")
	if len(parts) > 2 {
		cmdName = strings.Join(parts[1:2], " ")
	} else if len(parts) > 1 {
		cmdName = parts[1]
	}

	p.PostHog.Enqueue(posthog.Capture{
		DistinctId: p.Config.PersistentConfig.PlumberID,
		Event:      "batch",
		Properties: map[string]interface{}{
			"version":         options.VERSION,
			"command":         cmdName,
			"os":              runtime.GOOS,
			"$geoip_disabled": true,
		},
	})
}
