package validate

import (
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

const (
	GRPCCollectorAddress      = "grpc-collector.streamdal.com:9000"
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

	ErrMissingRelayOptions    = errors.New("relay options cannot be nil")
	ErrMissingCollectionToken = errors.New("collection token cannot be empty")
	ErrMissingConnectionId    = errors.New("connection id cannot be empty")
	ErrRelayNotFound          = errors.New("relay not found")
	ErrRelayNotActive         = errors.New("relay not active")
	ErrRelayAlreadyActive     = errors.New("relay already active")

	// Rule set

	ErrEmptyID     = errors.New("ruleset id in options cannot be empty")
	ErrEmptyKey    = errors.New("ruleset key in options cannot be empty")
	ErrEmptyBus    = errors.New("ruleset bus in options cannot be empty")
	ErrInvalidMode = errors.New("ruleset mode in options cannot be empty")
)

func RelayOptionsForServer(relayOptions *opts.RelayOptions) error {
	if relayOptions == nil {
		return ErrMissingRelayOptions
	}

	if relayOptions.CollectionToken == "" {
		return ErrMissingCollectionToken
	}

	if relayOptions.ConnectionId == "" {
		return ErrMissingConnectionId
	}

	if relayOptions.XStreamdalGrpcAddress == "" {
		relayOptions.XStreamdalGrpcAddress = GRPCCollectorAddress
	}

	if relayOptions.XStreamdalGrpcTimeoutSeconds == 0 {
		relayOptions.XStreamdalGrpcTimeoutSeconds = GRPCDefaultTimeoutSeconds
	}

	return nil
}

// ConnectionOptionsForServer ensures all required parameters are passed when
// creating/testing/updating a connection
func ConnectionOptionsForServer(connOptions *opts.ConnectionOptions) error {
	if connOptions == nil {
		return ErrMissingConnectionOptions
	}

	if connOptions.Name == "" {
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

func RuleSetForServer(rs *common.RuleSet) error {
	if rs.Id == "" {
		return ErrEmptyID
	}

	if rs.Key == "" {
		return ErrEmptyKey
	}

	if rs.Mode == 0 {
		return ErrInvalidMode
	}

	if rs.Bus == "" {
		return ErrEmptyBus
	}

	if len(rs.Rules) > 0 {
		// TODO: Implement specific rule validations
	}

	return nil
}
