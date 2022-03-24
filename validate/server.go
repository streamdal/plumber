package validate

import (
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

const (
	GRPCCollectorAddress      = "grpc-collector.batch.sh:9000"
	GRPCDefaultTimeoutSeconds = 5
)

var (
	// Server

	ErrMissingAuth  = errors.New("auth cannot be nil")
	ErrInvalidToken = errors.New("invalid token")

	// Connections

	ErrConnectionNotFound       = errors.New("connection not found")
	ErrMissingConnectionOptions = errors.New("connection options cannot be nil")
	ErrMissingConnName          = errors.New("you must provide a connection name")
	ErrMissingConnectionType    = errors.New("you must provide at least one connection of: kafka")

	// Reads

	ErrMissingReadOptions = errors.New("missing Read options")

	// Relay

	ErrRelayNotFound      = errors.New("relay not found")
	ErrRelayNotActive     = errors.New("relay not active")
	ErrRelayAlreadyActive = errors.New("relay already active")
)

func RelayOptionsForServer(relayOptions *opts.RelayOptions) error {
	if relayOptions == nil {
		return errors.New("relay options cannot be nil")
	}

	if relayOptions.CollectionToken == "" {
		return errors.New("collection token cannot be empty")
	}

	if relayOptions.ConnectionId == "" {
		return errors.New("connection id cannot be empty")
	}

	if relayOptions.XBatchshGrpcAddress == "" {
		relayOptions.XBatchshGrpcAddress = GRPCCollectorAddress
	}

	if relayOptions.XBatchshGrpcTimeoutSeconds == 0 {
		relayOptions.XBatchshGrpcTimeoutSeconds = GRPCDefaultTimeoutSeconds
	}

	return nil
}

// ConnectionOptionsForServer ensures all required parameters are passed when
// creating/testing/updating a connection
func ConnectionOptionsForServer(connOptions *opts.ConnectionOptions) error {
	if connOptions == nil {
		return ErrMissingConnectionOptions
	}

	if connOptions.Name == "" && connOptions.GetRabbit() == nil {
		return ErrMissingConnName
	}

	if connOptions.GetConn() == nil {
		return ErrMissingConnectionType
	}

	return nil
}

func TunnelOptionsForServer(tunnelOptions *opts.TunnelOptions) error {
	// TODO: Implement specific tunnel validations

	return nil
}
