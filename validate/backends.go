package validate

import "github.com/pkg/errors"

var (

	// Connection

	ErrMissingConnOpts   = errors.New("connection config cannot be nil")
	ErrMissingConnCfg    = errors.New("connection object in connection config cannot be nil")
	ErrMissingConnArgs   = errors.New("connection config args cannot be nil")
	ErrMissingClientKey  = errors.New("TLS key cannot be empty if TLS certificate is provided")
	ErrMissingClientCert = errors.New("TLS certificate cannot be empty if TLS key is provided")

	// Relay / Display

	ErrMissingMsg      = errors.New("msg cannot be nil")
	ErrMissingMsgValue = errors.New("msg.Value cannot be nil")

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
