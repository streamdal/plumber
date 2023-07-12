package actions

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/prometheus"
	"github.com/batchcorp/plumber/server/types"
	"github.com/batchcorp/plumber/validate"
)

func (a *Actions) CreateRelay(ctx context.Context, relayOpts *opts.RelayOptions) (*types.Relay, error) {
	if err := validate.RelayOptionsForServer(relayOpts); err != nil {
		return nil, errors.Wrap(err, "unable to validate relay options")
	}

	// Get stored connection information
	conn := a.cfg.PersistentConfig.GetConnection(relayOpts.ConnectionId)
	if conn == nil {
		return nil, validate.ErrConnectionNotFound
	}

	// Try to create a backend from given connection options
	be, err := backends.New(conn.Connection)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create backend")
	}

	// Used to shutdown relays on StopRelay() gRPC call
	shutdownCtx, shutdownFunc := context.WithCancel(context.Background())

	r := &types.Relay{
		Id:         relayOpts.XRelayId,
		Backend:    be,
		CancelFunc: shutdownFunc,
		CancelCtx:  shutdownCtx,
		Options:    relayOpts,
	}

	if relayOpts.XActive {
		if err := r.StartRelay(time.Millisecond * 100); err != nil {
			return nil, errors.Wrap(err, "unable to start relay")
		}

		r.Active = true
		r.Options.XActive = true

		// Update metrics
		prometheus.IncrPromGauge(prometheus.PlumberRelayWorkers)

	}

	a.cfg.PersistentConfig.SetRelay(r.Id, r)
	a.cfg.PersistentConfig.Save()

	return r, nil
}

func (a *Actions) StopRelay(ctx context.Context, relayID string) (*types.Relay, error) {
	if relayID == "" {
		return nil, errors.New("relayID cannot be empty")
	}

	relay := a.cfg.PersistentConfig.GetRelay(relayID)
	if relay == nil {
		return nil, validate.ErrRelayNotFound
	}

	if !relay.Active {
		return nil, validate.ErrRelayNotActive
	}

	// Stop worker
	relay.CancelFunc()

	relay.Active = false
	relay.Options.XActive = false

	// Update persistent storage
	a.cfg.PersistentConfig.SetRelay(relay.Id, relay)
	a.cfg.PersistentConfig.Save()

	// Update metrics
	prometheus.DecrPromGauge(prometheus.PlumberRelayWorkers)

	return relay, nil
}

func (a *Actions) ResumeRelay(ctx context.Context, relayID string) (*types.Relay, error) {
	if relayID == "" {
		return nil, errors.New("relayID cannot be empty")
	}

	relay := a.cfg.PersistentConfig.GetRelay(relayID)
	if relay == nil {
		return nil, validate.ErrRelayNotFound
	}

	if relay.Active {
		return nil, validate.ErrRelayAlreadyActive
	}

	if err := relay.StartRelay(time.Millisecond * 100); err != nil {
		return nil, errors.Wrap(err, "unable to start relay")
	}

	relay.Active = true
	relay.Options.XActive = true

	a.cfg.PersistentConfig.SetRelay(relayID, relay)
	a.cfg.PersistentConfig.Save()

	// Update metrics
	prometheus.IncrPromGauge(prometheus.PlumberRelayWorkers)

	return relay, nil
}

// DeleteRelay stops a relay (if active) and delete it from persistent storage
func (a *Actions) DeleteRelay(ctx context.Context, relayID string) (*types.Relay, error) {
	if relayID == "" {
		return nil, errors.New("relayID cannot be empty")
	}

	relay := a.cfg.PersistentConfig.GetRelay(relayID)
	if relay == nil {
		return nil, validate.ErrRelayNotFound
	}

	if relay.Active {
		a.log.Debugf("relay '%s' is active, stopping relay", relayID)

		relay.CancelFunc()

		relay.Active = false          // This shouldn't be needed but doing it in case the relay options show up in logs
		relay.Options.XActive = false // Same here

		// Update metrics
		prometheus.DecrPromGauge(prometheus.PlumberRelayWorkers)
	}

	// Delete from persistent storage
	a.cfg.PersistentConfig.DeleteRelay(relayID)
	a.cfg.PersistentConfig.Save()

	return relay, nil
}

func (a *Actions) UpdateRelay(ctx context.Context, relayID string, relayOpts *opts.RelayOptions) (*types.Relay, error) {
	relay := a.cfg.PersistentConfig.GetRelay(relayID)
	if relay == nil {
		return nil, errors.New("relay does not exist")
	}

	if relay.Active {
		a.log.Debugf("relay '%s' is active, stopping relay", relayID)

		// Close existing relay
		relay.CancelFunc()
		relay.Active = false
		relay.Options.XActive = false

		// Give it a sec to close out connections and goroutines
		time.Sleep(time.Second)

		prometheus.DecrPromGauge(prometheus.PlumberRelayWorkers)
	}

	// Get stored connection information
	conn := a.cfg.PersistentConfig.GetConnection(relayOpts.ConnectionId)
	if conn == nil {
		return nil, validate.ErrConnectionNotFound
	}

	// Try to create a backend from given connection options
	be, err := backends.New(conn.Connection)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create backend")
	}

	relay.Backend = be
	relay.Options = relayOpts

	// New contexts
	ctx, cancelFunc := context.WithCancel(context.Background())
	relay.CancelCtx = ctx
	relay.CancelFunc = cancelFunc

	if relayOpts.XActive {
		if err := relay.StartRelay(5 * time.Second); err != nil {
			relay.Options.XActive = false
			return nil, errors.Wrap(err, "unable to start relay")
		}

		relay.Active = true

	}

	// Update in-memory config
	a.cfg.PersistentConfig.SetRelay(relayID, relay)
	a.cfg.PersistentConfig.Save()

	// Update metrics
	prometheus.IncrPromGauge(prometheus.PlumberRelayWorkers)

	return relay, nil
}
