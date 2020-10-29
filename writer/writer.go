package writer

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/serializers"
)

// GenerateWriteValue will transform input data into the required format for transmission
func GenerateWriteValue(opts *cli.Options) ([]byte, error) {
	// Do we read value or file?
	var data []byte

	if opts.WriteInputData != "" {
		data = []byte(opts.WriteInputData)
	}

	if opts.WriteInputFile != "" {
		var readErr error

		data, readErr = ioutil.ReadFile(opts.WriteInputFile)
		if readErr != nil {
			return nil, fmt.Errorf("unable to read file '%s': %s", opts.WriteInputFile, readErr)
		}
	}

	// Handle AVRO
	if opts.AvroSchemaFile != "" {
		data, err := serializers.AvroEncode(opts.AvroSchemaFile, data)
		if err != nil {
			return nil, err
		}

		// Since AWS SQS works with strings only, we must convert it to base64
		if opts.AWSSQS.QueueName != "" {
			encoded := base64.StdEncoding.EncodeToString(data)
			return []byte(encoded), nil
		}

		return data, nil
	}

	// Input: Plain Output: Plain
	if opts.WriteInputType == "plain" && opts.WriteOutputType == "plain" {
		return data, nil
	}

	// Input: JSONPB Output: Protobuf
	if opts.WriteInputType == "jsonpb" && opts.WriteOutputType == "protobuf" {
		var convertErr error

		if opts.MsgDesc == nil {
			md, mdErr := pb.FindMessageDescriptor(opts.WriteProtobufDirs, opts.WriteProtobufRootMessage, opts.ProtobufDirRemap)
			if mdErr != nil {
				return nil, errors.Wrap(mdErr, "unable to find root message descriptor")
			}
			opts.MsgDesc = md
		}

		data, convertErr = convertJSONPBToProtobuf(data, dynamic.NewMessage(opts.MsgDesc))
		if convertErr != nil {
			return nil, errors.Wrap(convertErr, "unable to convert JSONPB to protobuf")
		}

		// Since AWS SQS works with strings only, we must convert it to base64
		if opts.AWSSQS.QueueName != "" {
			encoded := base64.StdEncoding.EncodeToString(data)
			return []byte(encoded), nil
		}
		return data, nil
	}

	// TODO: Input: Base64 Output: Plain
	// TODO: Input: Base64 Output: Protobuf
	// TODO: And a few more combinations ...

	return nil, errors.New("unsupported input/output combination")
}

// ValidateWriteOptions ensures that the correct flags and their values have been provided.
// Backends which require additional bus specific validation can pass them in via a closure
func ValidateWriteOptions(opts *cli.Options, busSpecific func(options *cli.Options) error) error {

	if busSpecific != nil {
		if err := busSpecific(opts); err != nil {
			return err
		}
	}

	// If type is protobuf, ensure both --protobuf-dir and --protobuf-root-message
	// are set as well
	if opts.WriteOutputType == "protobuf" {
		if err := cli.ValidateProtobufOptions(
			opts.WriteProtobufDirs,
			opts.WriteProtobufRootMessage,
		); err != nil {
			return fmt.Errorf("unable to validate protobuf option(s): %s", err)
		}
	}

	// InputData and file cannot be set at the same time
	if opts.WriteInputData != "" && opts.WriteInputFile != "" {
		return fmt.Errorf("--value and --file cannot both be set")
	}

	if opts.WriteInputFile != "" {
		if _, err := os.Stat(opts.WriteInputFile); os.IsNotExist(err) {
			return fmt.Errorf("--file '%s' does not exist", opts.WriteInputFile)
		}
	}

	return nil
}

// convertJSONPBToProtobuf converts input data from jsonpb -> protobuf -> bytes
func convertJSONPBToProtobuf(data []byte, m *dynamic.Message) ([]byte, error) {
	buf := bytes.NewBuffer(data)

	if err := jsonpb.Unmarshal(buf, m); err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal data into dynamic message")
	}

	// Now let's encode that into a proper protobuf message
	pbBytes, err := proto.Marshal(m)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal dynamic protobuf message to bytes")
	}

	return pbBytes, nil
}
