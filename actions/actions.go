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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/config"
	"github.com/batchcorp/plumber/server/types"
)

type Actions struct {
	cfg *Config
	log *logrus.Entry
}

type Config struct {
	PersistentConfig *config.Config
}

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 . IActions
type IActions interface {
	// relay
	CreateRelay(ctx context.Context, relayOpts *opts.RelayOptions) (*types.Relay, error)
	DeleteRelay(context.Context, string) (*types.Relay, error)
	StopRelay(ctx context.Context, relayID string) (*types.Relay, error)
	ResumeRelay(ctx context.Context, relayID string) (*types.Relay, error)
	UpdateRelay(ctx context.Context, relayID string, relayOpts *opts.RelayOptions) (*types.Relay, error)

	// tunnel
	CreateTunnel(reqCtx context.Context, tunnelOpts *opts.TunnelOptions) (*types.Tunnel, error)
	ResumeTunnel(ctx context.Context, tunnelID string) (*types.Tunnel, error)
	StopTunnel(ctx context.Context, tunnelID string) (*types.Tunnel, error)
	UpdateTunnel(ctx context.Context, tunnelID string, tunnelOpts *opts.TunnelOptions) (*types.Tunnel, error)
	DeleteTunnel(ctx context.Context, tunnelID string) error

	UpdateConnection(ctx context.Context, connectionID string, connOpts *opts.ConnectionOptions) (*types.Connection, error)
}

func New(cfg *Config) (IActions, error) {
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
