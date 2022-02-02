// Actions pkg exists so that we have a place to store methods that are called
// by either the gRPC server, etcd broadcast consumer or both.
//
// This pkg generally houses server-related methods. It should NOT be used for
// performing etcd related functionality (to avoid circular import issues).
//
// NOTE: We should only emit relay worker metrics _only_ from here.

package actions

import (
	"context"
	"time"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/config"
	"github.com/batchcorp/plumber/prometheus"
	"github.com/batchcorp/plumber/server/types"
	"github.com/batchcorp/plumber/validate"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Actions struct {
	cfg *Config
	log *logrus.Entry
}

type Config struct {
	PersistentConfig *config.Config
}

func New(cfg *Config) (*Actions, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to validate config")
	}

	return &Actions{
		cfg: cfg,
		log: logrus.WithField("pkg", "actions"),
	}, nil
}

func validateConfig(cfg *Config) error {
	if cfg == nil {
		return errors.New("config cannot be nil")
	}

	if cfg.PersistentConfig == nil {
		return errors.New("config.PersistentConfig cannot be nil")
	}

	return nil
}

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
	be, err := backends.New(conn)
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

	// Update metrics
	prometheus.DecrPromGauge(prometheus.PlumberRelayWorkers)

	return relay, nil
}

func (a *Actions) ResumeReplay(ctx context.Context, relayID string) (*types.Relay, error) {
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

	return relay, nil
}
