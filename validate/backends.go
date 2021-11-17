package validate

import "github.com/pkg/errors"

var (

	// Connection

	ErrMissingConnOpts = errors.New("connection config cannot be nil")
	ErrMissingConnCfg  = errors.New("connection object in connection config cannot be nil")
	ErrMissingConnArgs = errors.New("connection config args cannot be nil")

	// Read

	// Write

	ErrEmptyWriteOpts = errors.New("write options cannot be nil")

	// Dynamic

	ErrEmptyDynamicOpts = errors.New("dynamic options cannot be nil")

	// Relay

	ErrEmptyRelayOpts = errors.New("relay options cannot be nil")

	// Shared

	ErrEmptyBackendGroup = errors.New("backend group options cannot be nil")
	ErrEmptyBackendArgs  = errors.New("backend arg options cannot be nil")
)
