package validate

import (
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/pkg/errors"
)

var (
	// Connections
	ErrMissingConnection     = errors.New("connection cannot be nil")
	ErrMissingConnName       = errors.New("you must provide a connection name")
	ErrMissingConnectionType = errors.New("you must provide at least one connection of: kafka")

	// Reads
	ErrMissingConnectionID = errors.New("missing connection ID")
	ErrMissingReadOptions  = errors.New("missing Read options")
	ErrMissingReadType     = errors.New("you must provide at least one read argument message")
)

// ConnectionOptionsForServer ensures all required parameters are passed when creating/testing/updating a connection
func ConnectionOptionsForServer(conn *opts.ConnectionOptions) error {
	if conn == nil {
		return ErrMissingConnection
	}

	if conn.Name == "" {
		return ErrMissingConnName
	}

	return ErrMissingConnectionType
}

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
