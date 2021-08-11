package plumber

import (
	"fmt"
	"strings"

	"github.com/batchcorp/plumber/backends/batch"
)

// HandleBatchCmd handles all commands related to Batch.sh API
func (p *Plumber) HandleBatchCmd() error {
	b := batch.New(p.Options, p.PersistentConfig)

	switch {
	case p.Cmd == "batch login":
		return b.Login()
	case p.Cmd == "batch logout":
		return b.Logout()
	case p.Cmd == "batch list collection":
		return b.ListCollections()
	case p.Cmd == "batch create collection":
		return b.CreateCollection()
	case p.Cmd == "batch list destination":
		return b.ListDestinations()
	case strings.HasPrefix(p.Cmd, "batch create destination"):
		commands := strings.Split(p.Cmd, " ")
		return b.CreateDestination(commands[3])
	case p.Cmd == "batch list schema":
		return b.ListSchemas()
	case p.Cmd == "batch list replay":
		return b.ListReplays()
	case p.Cmd == "batch create replay":
		return b.CreateReplay()
	case p.Cmd == "batch archive replay":
		return b.ArchiveReplay()
	case p.Cmd == "batch search":
		return b.SearchCollection()
	default:
		return fmt.Errorf("unrecognized command: %s", p.Cmd)
	}
}
