package validate

import (
	"net/url"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/pkg/errors"
)

var (
	// Backend

	ErrBackendNotFound = errors.New("backend not found")

	// Server

	ErrMissingAuth  = errors.New("auth cannot be nil")
	ErrInvalidToken = errors.New("invalid token")

	// Connections

	ErrMissingConnectionOptions = errors.New("connection options cannot be nil")
	ErrMissingAddress           = errors.New("at least one kafka server address must be specified")
	ErrMissingUsername          = errors.New("you must provide a username when specifying a SASL type")
	ErrMissingPassword          = errors.New("you must provide a password when specifying a SASL type")
	ErrMissingConnName          = errors.New("you must provide a connection name")
	ErrMissingConnectionType    = errors.New("you must provide at least one connection of: kafka")

	// Reads

	ErrMissingConnectionID      = errors.New("missing connection ID")
	ErrMissingReadOptions       = errors.New("missing Read options")
	ErrMissingReadType          = errors.New("you must provide at least one read argument message")
	ErrMissingTopic             = errors.New("you must provide at least one topic to read from")
	ErrMissingConsumerGroupName = errors.New("group name must be specified when using a consumer group")
	ErrMissingRootType          = errors.New("root message cannot be empty")
	ErrMissingZipArchive        = errors.New("zip archive is empty")
	ErrMissingAVROSchema        = errors.New("AVRO schema cannot be empty")
	ErrMissingKafkaArgs         = errors.New("you must provide at least one arguments of: kafka")

	// Services

	ErrMissingName     = errors.New("name cannot be empty")
	ErrMissingOwner    = errors.New("owner cannot be empty")
	ErrMissingService  = errors.New("service cannot be empty")
	ErrInvalidRepoURL  = errors.New("repo URL must be a valid URL or left blank")
	ErrServiceNotFound = errors.New("service does not exist")

	// Schemas

	ErrInvalidGithubSchemaType = errors.New("only protobuf and avro schemas can be imported from github")
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

	return ErrMissingReadType
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

// TODO: Implement
func RelayOptionsForServer(relayOptions *opts.RelayOptions) error {
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

	if s.RepoUrl != "" {
		_, err := url.ParseRequestURI(s.RepoUrl)
		if err != nil {
			return ErrInvalidRepoURL
		}
	}

	return nil
}

func WriteOptionsForServer(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return errors.New("write options cannot be nil")
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
