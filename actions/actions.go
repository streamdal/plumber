// Actions pkg exists so that we have a place to store methods that are called
// by either the gRPC server, etcd broadcast consumer or both.
//
// This pkg generally houses server-related methods. It should NOT be used for
// performing etcd related functionality (to avoid circular import issues).
//
// NOTE: We should only emit relay worker metrics _only_ from here.

package actions

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/config"
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
