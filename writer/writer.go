package writer

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/serializers"
)

func GenerateWriteValues(md *desc.MessageDescriptor, opts *cli.Options) ([][]byte, error) {
	writeValues := make([][]byte, 0)

	// File source
	if opts.WriteInputFile != "" {
		data, err := ioutil.ReadFile(opts.WriteInputFile)
		if err != nil {
			return nil, fmt.Errorf("unable to read file '%s': %s", opts.WriteInputFile, err)
		}

		wv, err := generateWriteValue(data, md, opts)
		if err != nil {
			return nil, err
		}

		writeValues = append(writeValues, wv)
		return writeValues, nil
	}

	// Stdin source
	for _, data := range opts.WriteInputData {
		wv, err := generateWriteValue([]byte(data), md, opts)
		if err != nil {
			return nil, err
		}

		writeValues = append(writeValues, wv)
	}

	return writeValues, nil
}

// generateWriteValue will transform input data into the required format for transmission
func generateWriteValue(data []byte, md *desc.MessageDescriptor, opts *cli.Options) ([]byte, error) {
	// Ensure we do not try to operate on a nil md
	if opts.WriteInputFile == "jsonpb" && md == nil {
		return nil, errors.New("message descriptor cannot be nil when --input-type is jsonpb")
	}

	// Handle AVRO
	if opts.AvroSchemaFile != "" {
		data, err := serializers.AvroEncodeWithSchemaFile(opts.AvroSchemaFile, data)
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
	if opts.WriteInputType == "plain" {
		return data, nil
	}

	// Input: JSONPB Output: Protobuf
	if opts.WriteInputType == "jsonpb" {
		var convertErr error

		data, convertErr = ConvertJSONPBToProtobuf(data, dynamic.NewMessage(md))
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

	return nil, errors.New("unsupported --input-type")
}

// ValidateWriteOptions ensures that the correct flags and their values have been provided.
// Backends which require additional bus specific validation can pass them in via a closure
func ValidateWriteOptions(opts *cli.Options, busSpecific func(options *cli.Options) error) error {
	if busSpecific != nil {
		if err := busSpecific(opts); err != nil {
			return err
		}
	}

	if len(opts.WriteInputData) == 0 && opts.WriteInputFile == "" {
		return errors.New("either --input-data or --input-file must be specified")
	}

	// InputData and file cannot be set at the same time
	if len(opts.WriteInputData) > 0 && opts.WriteInputFile != "" {
		return fmt.Errorf("--input-data and --input-file cannot both be set")
	}

	if opts.WriteInputFile != "" {
		if _, err := os.Stat(opts.WriteInputFile); os.IsNotExist(err) {
			return fmt.Errorf("--file '%s' does not exist", opts.WriteInputFile)
		}
	}

	return nil
}

// ConvertJSONPBToProtobuf converts input data from jsonpb -> protobuf -> bytes
func ConvertJSONPBToProtobuf(data []byte, m *dynamic.Message) ([]byte, error) {
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
