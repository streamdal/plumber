// Package validate contains various validation functions
package validate

import (
	"fmt"
	"os"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/streamdal/plumber/util"
)

var (
	ErrMissingCLIOptions = errors.New("cli options cannot be nil")
)

func ProtobufOptionsForCLI(dirs []string, rootMessage, fdsFile string) error {
	if len(dirs) == 0 && fdsFile == "" {
		return errors.New("at least one '--protobuf-dirs' or --protobuf-descriptor-set required when type " +
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

func RelayOptionsForCLI(relayOpts *opts.RelayOptions) error {
	if relayOpts == nil {
		return ErrEmptyRelayOpts
	}

	if relayOpts.XCliOptions == nil {
		return ErrMissingCLIOptions
	}

	return nil
}

func ReadOptionsForCLI(readOpts *opts.ReadOptions) error {
	if readOpts == nil {
		return ErrMissingReadOptions
	}

	if readOpts.XCliOptions == nil {
		return ErrMissingCLIOptions
	}

	return nil
}

func WriteOptionsForCLI(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return ErrEmptyWriteOpts
	}

	if writeOpts.XCliOptions == nil {
		return ErrMissingCLIOptions
	}

	if writeOpts.Record.Input == "" && writeOpts.XCliOptions.InputFile == "" && len(writeOpts.XCliOptions.InputStdin) == 0 {
		return errors.New("either --input or --input-file or  must be specified")
	}

	// Input and file cannot be set at the same time
	if len(writeOpts.Record.Input) > 0 && writeOpts.XCliOptions.InputFile != "" {
		return fmt.Errorf("--input-data and --input-file cannot both be set")
	}

	if writeOpts.XCliOptions.InputFile != "" {
		if _, err := os.Stat(writeOpts.XCliOptions.InputFile); os.IsNotExist(err) {
			return fmt.Errorf("--input-file '%s' does not exist", writeOpts.XCliOptions.InputFile)
		}
	}

	if writeOpts.EncodeOptions != nil {
		// Protobuf
		if writeOpts.EncodeOptions.EncodeType == encoding.EncodeType_ENCODE_TYPE_JSONPB {
			pbSettings := writeOpts.EncodeOptions.ProtobufSettings
			if pbSettings == nil {
				return errors.New("protobuf settings cannot be unset if encode type is set to jsonpb")
			}

			if pbSettings.ProtobufRootMessage == "" {
				return errors.New("protobuf root message must be set if encode type is set to jsonpb")
			}

			if len(pbSettings.ProtobufDirs) == 0 && pbSettings.ProtobufDescriptorSet == "" {
				return errors.New("either a protobuf directory or a descriptor set file must be specified if encode type is set to jsonpb")
			}
		}

		// Avro
		if writeOpts.EncodeOptions.EncodeType == encoding.EncodeType_ENCODE_TYPE_AVRO {
			if writeOpts.EncodeOptions.AvroSettings == nil {
				return errors.New("avro settings cannot be nil if encode type is set to avro")
			}

			if writeOpts.EncodeOptions.AvroSettings.AvroSchemaFile == "" {
				return errors.New("avro schema file must be specified if encode type is set to avro")
			}
		}
	}

	return nil
}
