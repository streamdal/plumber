package plumber

import (
	"fmt"
	"strings"

	"github.com/batchcorp/plumber/backends/batch"
)

// HandleBatchCmd handles all commands related to Batch.sh API
func (p *Plumber) HandleBatchCmd() error {
	b := batch.New(p.CLIOptions, p.PersistentConfig)

	switch {
	case p.CLIOptions.Global.XFullCommand == "batch login":
		return b.Login()
	case p.CLIOptions.Global.XFullCommand == "batch logout":
		return b.Logout()
	case p.CLIOptions.Global.XFullCommand == "batch list collection":
		return b.ListCollections()
	case p.CLIOptions.Global.XFullCommand == "batch create collection":
		return b.CreateCollection()
	case p.CLIOptions.Global.XFullCommand == "batch list destination":
		return b.ListDestinations()
	case strings.HasPrefix(p.CLIOptions.Global.XFullCommand, "batch create destination"):
		commands := strings.Split(p.CLIOptions.Global.XFullCommand, " ")
		return b.CreateDestination(commands[3])
	case p.CLIOptions.Global.XFullCommand == "batch list schema":
		return b.ListSchemas()
	case p.CLIOptions.Global.XFullCommand == "batch list replay":
		return b.ListReplays()
	case p.CLIOptions.Global.XFullCommand == "batch create replay":
		return b.CreateReplay()
	case p.CLIOptions.Global.XFullCommand == "batch archive replay":
		return b.ArchiveReplay()
	case p.CLIOptions.Global.XFullCommand == "batch search":
		return b.SearchCollection()
	default:
		return fmt.Errorf("unrecognized command: %s", p.KongCtx)
	}
}
