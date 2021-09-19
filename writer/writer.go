package writer

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io/ioutil"

	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/serializers"
)

// GenerateWriteValue generates a slice of WriteRecords that can be passed to
// backends to perform a write.
func GenerateWriteValue(writeOpts *opts.WriteOptions, md *desc.MessageDescriptor) ([]*records.WriteRecord, error) {
	writeValues := make([]*records.WriteRecord, 0)

	if writeOpts == nil {
		return nil, errors.New("write opts cannot be nil")
	}

	if writeOpts.Record == nil {
		return nil, errors.New("write opts record cannot be nil")
	}

	// Input already provided
	if writeOpts.Record.Input != "" {
		wv, err := generateWriteValue([]byte(writeOpts.Record.Input), writeOpts, md)
		if err != nil {
			return nil, err
		}

		writeValues = append(writeValues, &records.WriteRecord{
			Input:         string(wv),
			InputMetadata: writeOpts.Record.InputMetadata,
		})

		return writeValues, nil
	}

	// If server, the only possible input is Record.Input
	if writeOpts.XCliOptions == nil {
		return nil, errors.New("no input found - unable to generate write value")
	}

	// File source
	if writeOpts.XCliOptions.InputFile != "" {
		data, err := ioutil.ReadFile(writeOpts.XCliOptions.InputFile)
		if err != nil {
			return nil, fmt.Errorf("unable to read file '%s': %s", writeOpts.XCliOptions.InputFile, err)
		}

		wv, err := generateWriteValue(data, writeOpts, md)
		if err != nil {
			return nil, err
		}

		writeValues = append(writeValues, &records.WriteRecord{
			Input:         string(wv),
			InputMetadata: writeOpts.Record.InputMetadata,
		})

		return writeValues, nil
	}

	// TODO: This is kind of lame - stdin should probably just go straight into
	// WriteOpts.Record.Input (and make Input into repeated string)

	// Stdin source
	for _, data := range writeOpts.XCliOptions.InputStdin {
		wv, err := generateWriteValue([]byte(data), writeOpts, md)
		if err != nil {
			return nil, err
		}

		writeValues = append(writeValues, &records.WriteRecord{
			Input:         string(wv),
			InputMetadata: writeOpts.Record.InputMetadata,
		})
	}

	if len(writeValues) == 0 {
		return nil, errors.New("exhausted all input sources - no input found")
	}

	return writeValues, nil
}

// generateWriteValue will transform input data into the required format for transmission
func generateWriteValue(data []byte, writeOpts *opts.WriteOptions, md *desc.MessageDescriptor) ([]byte, error) {
	// Input: Plain / unset
	if writeOpts.EncodeOptions == nil ||
		writeOpts.EncodeOptions.EncodeType == encoding.EncodeType_ENCODE_TYPE_UNSET {

		return data, nil
	}

	// Input: AVRO
	if writeOpts.EncodeOptions.AvroSettings.AvroSchemaFile != "" {
		data, err := serializers.AvroEncodeWithSchemaFile(writeOpts.EncodeOptions.AvroSettings.AvroSchemaFile, data)
		if err != nil {
			return nil, err
		}

		// Since AWS SQS works with strings only, we must convert it to base64
		if writeOpts.Awssqs != nil && writeOpts.Awssqs.Args.QueueName != "" {
			encoded := base64.StdEncoding.EncodeToString(data)
			return []byte(encoded), nil
		}

		return data, nil
	}

	// Input: JSONPB
	if writeOpts.EncodeOptions.EncodeType == encoding.EncodeType_ENCODE_TYPE_JSONPB {
		var convertErr error

		if md == nil {
			return nil, errors.New("message descriptor cannot be nil")
		}

		data, convertErr = ConvertJSONPBToProtobuf(data, dynamic.NewMessage(md))
		if convertErr != nil {
			return nil, errors.Wrap(convertErr, "unable to convert JSONPB to protobuf")
		}

		// Since AWS SQS works with strings only, we must convert it to base64
		if writeOpts.Awssqs != nil && writeOpts.Awssqs.Args.QueueName != "" {
			encoded := base64.StdEncoding.EncodeToString(data)
			return []byte(encoded), nil
		}
		return data, nil
	}

	return nil, errors.New("unsupported --input-type")
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
