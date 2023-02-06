package plumber

import (
	"fmt"
	"strings"

	"github.com/posthog/posthog-go"

	"github.com/batchcorp/plumber/backends/streamdal"
)

// HandleStreamdalCmd handles all commands related to Streamdal API
func (p *Plumber) HandleStreamdalCmd() error {
	b := streamdal.New(p.CLIOptions, p.PersistentConfig)

	// Less typing
	cmd := p.CLIOptions.Global.XFullCommand

	p.Telemetry.Enqueue(posthog.Capture{
		Event:      "cli_batch",
		DistinctId: p.PersistentConfig.PlumberID,
		Properties: map[string]interface{}{
			"command": cmd,
		},
	})

	switch {
	case strings.HasPrefix(cmd, "streamdal login"):
		return b.Login()
	case strings.HasPrefix(cmd, "streamdal logout"):
		return b.Logout()
	case strings.HasPrefix(cmd, "streamdal list collection"):
		return b.ListCollections()
	case strings.HasPrefix(cmd, "streamdal create collection"):
		return b.CreateCollection()
	case strings.HasPrefix(cmd, "streamdal create destination"):
		commands := strings.Split(cmd, " ")
		return b.CreateDestination(commands[3])
	case strings.HasPrefix(cmd, "streamdal list destination"):
		return b.ListDestinations()
	case strings.HasPrefix(cmd, "streamdal list schema"):
		return b.ListSchemas()
	case strings.HasPrefix(cmd, "streamdal list replay"):
		return b.ListReplays()
	case strings.HasPrefix(cmd, "streamdal create replay"):
		return b.CreateReplay()
	case strings.HasPrefix(cmd, "streamdal archive replay"):
		return b.ArchiveReplay()
	case strings.HasPrefix(cmd, "streamdal search"):
		return b.SearchCollection()
	default:
		return fmt.Errorf("unrecognized command: %s", p.CLIOptions.Global.XFullCommand)
	}
}
