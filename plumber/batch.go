package plumber

import (
	"fmt"
	"strings"

	"github.com/batchcorp/plumber/backends/batch"
)

// HandleBatchCmd handles all commands related to Batch.sh API
func (p *Plumber) HandleBatchCmd() error {
	b := batch.New(p.CLIOptions, p.PersistentConfig)

	if strings.HasPrefix(p.CLIOptions.Global.XFullCommand, "batch create destination") {
		commands := strings.Split(p.CLIOptions.Global.XFullCommand, " ")
		return b.CreateDestination(commands[3])
	}

	switch p.CLIOptions.Global.XFullCommand {
	case "batch login":
		return b.Login()
	case "batch logout":
		return b.Logout()
	case "batch list collection":
		return b.ListCollections()
	case "batch create collection":
		return b.CreateCollection()
	case "batch list destination":
		return b.ListDestinations()
	case "batch list schema":
		return b.ListSchemas()
	case "batch list replay":
		return b.ListReplays()
	case "batch create replay":
		return b.CreateReplay()
	case "batch archive replay":
		return b.ArchiveReplay()
	case "batch search":
		return b.SearchCollection()
	default:
		return fmt.Errorf("unrecognized command: %s", p.CLIOptions.Global.XFullCommand)
	}
}
