// Package validate contains various validation functions
package validate

import (
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
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

func ReadOptions(readOpts *opts.ReadOptions) error {
	if readOpts == nil {
		return errors.New("read options cannot be nil")
	}

	if readOpts.XCliOptions == nil {
		return errors.New("CLI options cannot be nil")
	}

	return nil
}

func WriteOptions(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return errors.New("write options cannot be nil")
	}

	if writeOpts.XCliOptions == nil {
		return errors.New("cli options cannot be nil")
	}

	if writeOpts.EncodeOptions != nil {
		// Protobuf
		if writeOpts.EncodeOptions.EncodeType == encoding.EncodeType_ENCODE_TYPE_JSONPB {
			if writeOpts.EncodeOptions.ProtobufSettings == nil {
				return errors.New("protobuf settings cannot be unset if encode type is set to jsonpb")
			}

			if writeOpts.EncodeOptions.ProtobufSettings.ProtobufRootMessage == "" {
				return errors.New("protobuf root message must be set if encode type is set to jsonpb")
			}

			if len(writeOpts.EncodeOptions.ProtobufSettings.ProtobufDirs) == 0 {
				return errors.New("at least one protobuf dir must be specified if encode type is set to jsonpb")
			}
		}

		// Avro
		if writeOpts.EncodeOptions.EncodeType == encoding.EncodeType_ENCODE_TYPE_AVRO {
			if writeOpts.EncodeOptions.AvroSchemaFile == "" {
				return errors.New("avro schema file must be specified if encode type is set to avro")
			}
		}
	}

	return nil
}
