package plumber

import (
	"fmt"
	"strings"

	"github.com/batchcorp/plumber/backends/batch"
	"github.com/posthog/posthog-go"
)

// HandleBatchCmd handles all commands related to Batch.sh API
func (p *Plumber) HandleBatchCmd() error {
	b := batch.New(p.CLIOptions, p.PersistentConfig)

	// Less typing
	cmd := p.CLIOptions.Global.XFullCommand

	p.Telemetry.AsyncEnqueue(posthog.Capture{
		Event:      "cli_batch",
		DistinctId: p.PersistentConfig.PlumberID,
		Properties: map[string]interface{}{
			"command": cmd,
		},
	})

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
