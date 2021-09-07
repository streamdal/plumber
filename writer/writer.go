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

	"github.com/batchcorp/plumber/types"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/serializers"
)

func GenerateWriteMessageFromOptions(opts *options.Options) ([]*types.WriteMessage, error) {
	writeValues := make([]*types.WriteMessage, 0)

	// File source
	if opts.Write.InputFile != "" {
		data, err := ioutil.ReadFile(opts.Write.InputFile)
		if err != nil {
			return nil, fmt.Errorf("unable to read file '%s': %s", opts.Write.InputFile, err)
		}

		wv, err := generateWriteValue(data, opts)
		if err != nil {
			return nil, err
		}

		writeValues = append(writeValues, &types.WriteMessage{
			Value: wv,
		})

		return writeValues, nil
	}

	// Stdin source
	for _, data := range opts.Write.InputData {
		wv, err := generateWriteValue([]byte(data), opts)
		if err != nil {
			return nil, err
		}

		writeValues = append(writeValues, &types.WriteMessage{
			Value: wv,
		})
	}

	return writeValues, nil
}

// generateWriteValue will transform input data into the required format for transmission
func generateWriteValue(data []byte, opts *options.Options) ([]byte, error) {
	// New AVRO
	if opts.Decoding.AvroSchemaFile != "" {
		data, err := serializers.AvroEncodeWithSchemaFile(opts.Decoding.AvroSchemaFile, data)
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
	if opts.Write.InputType == "plain" {
		return data, nil
	}

	// Input: JSONPB Output: Protobuf
	if opts.Write.InputType == "jsonpb" {
		var convertErr error

		data, convertErr = ConvertJSONPBToProtobuf(data, dynamic.NewMessage(opts.Encoding.MsgDesc))
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
func ValidateWriteOptions(opts *options.Options, busSpecific func(options *options.Options) error) error {
	if busSpecific != nil {
		if err := busSpecific(opts); err != nil {
			return err
		}
	}

	if len(opts.Write.InputData) == 0 && opts.Write.InputFile == "" {
		return errors.New("either --input-data or --input-file must be specified")
	}

	// InputData and file cannot be set at the same time
	if len(opts.Write.InputData) > 0 && opts.Write.InputFile != "" {
		return fmt.Errorf("--input-data and --input-file cannot both be set")
	}

	if opts.Write.InputFile != "" {
		if _, err := os.Stat(opts.Write.InputFile); os.IsNotExist(err) {
			return fmt.Errorf("--file '%s' does not exist", opts.Write.InputFile)
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
