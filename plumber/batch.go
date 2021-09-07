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
	case p.KongCtx == "batch login":
		return b.Login()
	case p.KongCtx == "batch logout":
		return b.Logout()
	case p.KongCtx == "batch list collection":
		return b.ListCollections()
	case p.KongCtx == "batch create collection":
		return b.CreateCollection()
	case p.KongCtx == "batch list destination":
		return b.ListDestinations()
	case strings.HasPrefix(p.KongCtx, "batch create destination"):
		commands := strings.Split(p.KongCtx, " ")
		return b.CreateDestination(commands[3])
	case p.KongCtx == "batch list schema":
		return b.ListSchemas()
	case p.KongCtx == "batch list replay":
		return b.ListReplays()
	case p.KongCtx == "batch create replay":
		return b.CreateReplay()
	case p.KongCtx == "batch archive replay":
		return b.ArchiveReplay()
	case p.KongCtx == "batch search":
		return b.SearchCollection()
	default:
		return fmt.Errorf("unrecognized command: %s", p.KongCtx)
	}
}
