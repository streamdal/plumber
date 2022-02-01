package actions

import (
	"context"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/config"
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

	if err := r.StartRelay(); err != nil {
		return nil, errors.Wrap(err, "unable to start relay")
	}

	r.Active = true

	a.cfg.PersistentConfig.SetRelay(r.Id, r)

	return r, nil
}
