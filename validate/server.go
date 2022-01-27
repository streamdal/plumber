package validate

import (
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

const (
	GRPCCollectorAddress      = "grpc-collector.batch.sh:9000"
	GRPCDefaultTimeoutSeconds = 5
)

var (
	// Backend

	ErrBackendNotFound = errors.New("backend not found")

	// Server

	ErrMissingAuth  = errors.New("auth cannot be nil")
	ErrInvalidToken = errors.New("invalid token")

	// Connections

	ErrConnectionNotFound       = errors.New("connection not found")
	ErrMissingConnectionOptions = errors.New("connection options cannot be nil")
	ErrMissingConnName          = errors.New("you must provide a connection name")
	ErrMissingConnectionType    = errors.New("you must provide at least one connection of: kafka")

	// Reads

	ErrMissingConnectionID = errors.New("missing connection ID")
	ErrMissingReadOptions  = errors.New("missing Read options")
	ErrMissingReadType     = errors.New("you must provide at least one read argument message")

	// Services

	ErrMissingName     = errors.New("name cannot be empty")
	ErrMissingService  = errors.New("service cannot be empty")
	ErrInvalidRepoURL  = errors.New("repo URL must be a valid URL or left blank")
	ErrServiceNotFound = errors.New("service does not exist")
	ErrRepoNotFound    = errors.New("repository was not found on this service")

	// Schemas

	ErrInvalidGithubSchemaType = errors.New("only protobuf and avro schemas can be imported from github")
	ErrSchemaNotFound          = errors.New("schema does not exist")

	// Composite views

	ErrMissingComposite  = errors.New("composite cannot be empty")
	ErrCompositeReadIDs  = errors.New("you must specify at least 2 read IDs for a composite view")
	ErrCompositeNotFound = errors.New("composite view does not exist")
)

func ReadOptionsForServer(readOptions *opts.ReadOptions) error {
	if readOptions == nil {
		return ErrMissingReadOptions
	}

	if readOptions.ConnectionId == "" {
		return ErrMissingConnectionID
	}

	// Each backend performs its own validations - no need to do it twice

	if err := SamplingOptionsForServer(readOptions.SampleOptions); err != nil {
		return errors.Wrap(err, "unable to validate sampling options")
	}

	if err := DecodeOptionsForServer(readOptions.DecodeOptions); err != nil {
		return errors.Wrap(err, "unable to validate decode options")
	}

	return nil
}

func SamplingOptionsForServer(sampleOptions *opts.ReadSampleOptions) error {
	// Sampling options are optional
	if sampleOptions == nil {
		return nil
	}

	if sampleOptions.SampleRate == 0 {
		return errors.New("sampling rate must be >0")
	}

	if sampleOptions.SampleIntervalSeconds == 0 {
		return errors.New("sampling interval must be >0")
	}

	return nil
}

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

func DecodeOptionsForServer(decodeOptions *encoding.DecodeOptions) error {
	// Decode options are optional
	if decodeOptions == nil {
		return nil
	}

	// No need to perform other decoding checks if schema is provided, however
	// schema verification occurs outside this function.
	if decodeOptions.SchemaId != "" {
		return nil
	}

	// If we are here, we were not given a schema ID - we are using the provided
	// decoding options.

	// Protobuf
	if decodeOptions.DecodeType == encoding.DecodeType_DECODE_TYPE_PROTOBUF {
		if decodeOptions.ProtobufSettings == nil {
			return errors.New("protobuf settings cannot be nil when decode type is protobuf")
		}

		if decodeOptions.ProtobufSettings.ProtobufRootMessage == "" {
			return errors.New("protobuf root message cannot be empty when decode type is protobuf")
		}

		if len(decodeOptions.ProtobufSettings.Archive) == 0 {
			return errors.New("protobuf archive cannot be empty when decode type is protobuf")
		}
	}

	if decodeOptions.DecodeType == encoding.DecodeType_DECODE_TYPE_AVRO {
		if decodeOptions.AvroSettings == nil {
			return errors.New("avro settings cannot be nil when decode type is set to avro")
		}

		if len(decodeOptions.AvroSettings.Schema) == 0 {
			return errors.New("avro schema cannot be empty when decode type is set to avro")
		}
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

func ServiceForServer(s *protos.Service) error {
	if s == nil {
		return ErrMissingService
	}

	if s.Name == "" {
		return ErrMissingName
	}

	//if s.OwnerId == "" {
	//	return ErrMissingOwner
	//}

	return nil
}

func WriteOptionsForServer(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return ErrEmptyWriteOpts
	}

	if writeOpts.Record == nil {
		return errors.New("record in write opts cannot be nil")
	}

	if writeOpts.ConnectionId == "" {
		return errors.New("connection id cannot be empty")
	}

	if writeOpts.Record.Input == "" {
		return errors.New("record input cannot be empty")
	}

	// OK to not have any encode options
	if writeOpts.EncodeOptions != nil {
		if err := EncodeOptionsForServer(writeOpts.EncodeOptions); err != nil {
			return errors.Wrap(err, "unable to validate encode options")
		}
	}

	return nil
}

func EncodeOptionsForServer(encodeOptions *encoding.EncodeOptions) error {
	if encodeOptions == nil {
		return nil
	}

	// TODO: Implement specific encode validations

	return nil
}

func CompositeOptionsForServer(comp *opts.Composite) error {
	if comp == nil {
		return ErrMissingComposite
	}

	if len(comp.ReadIds) < 2 {
		return ErrCompositeReadIDs
	}

	return nil
}
