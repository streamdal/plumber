// Package validate contains various validation functions
package validate

import (
	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
)

func ProtobufOptions(dirs []string, rootMessage string) error {
	if len(dirs) == 0 {
		return errors.New("at least one '--protobuf-dir' required when type " +
			"is set to 'protobuf'")
	}

	if rootMessage == "" {
		return errors.New("'--protobuf-root-message' required when " +
			"type is set to 'protobuf'")
	}

	// Does given dir exist?
	if err := util.DirsExist(dirs); err != nil {
		return errors.Wrap(err, "--protobuf-dir validation error(s)")
	}

	return nil
}

func ReadConfig(cfg *protos.ReadConfig) error {
	if cfg == nil {
		return errors.New("read config cannot be nil")
	}

	if cfg.ReadOpts == nil {
		return errors.New("read options cannot be nil")
	}

	if cfg.XCliConfig == nil {
		return errors.New("CLI config cannot be nil")
	}

	return nil
}
