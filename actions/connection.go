package actions

import (
	"context"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/streamdal/plumber/server/types"
)

func (a *Actions) UpdateConnection(_ context.Context, connectionID string, connOpts *opts.ConnectionOptions) (*types.Connection, error) {

	conn := &types.Connection{Connection: connOpts}

	// Update connection in persistent config
	a.cfg.PersistentConfig.SetConnection(connectionID, conn)
	_ = a.cfg.PersistentConfig.Save()

	// Starting/stopping needs to lock this mutex, so copy it for access
	a.cfg.PersistentConfig.RelaysMutex.RLock()
	relays := make(map[string]*types.Relay)
	for k, v := range a.cfg.PersistentConfig.Relays {
		relays[k] = v
	}
	a.cfg.PersistentConfig.RelaysMutex.RUnlock()

	// Restart all relays that use this connection and are active
	// Inactive relays will pick up the new connection details whenever they get resumed
	for _, relay := range relays {
		if relay.Options.ConnectionId == connectionID && relay.Active {
			// Don't use the request context, use a fresh one
			if _, err := a.StopRelay(context.Background(), relay.Options.XRelayId); err != nil {
				a.log.Errorf("unable to stop relay '%s': %s", relay.Options.XRelayId, err)
				continue
			}

			// Don't use the request context, use a fresh one
			if _, err := a.ResumeRelay(context.Background(), relay.Options.XRelayId); err != nil {
				a.log.Errorf("unable to resume relay '%s': %s", relay.Options.XRelayId, err)
				continue
			}
		}
	}

	return conn, nil
}
