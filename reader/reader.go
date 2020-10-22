package reader

import (
	"encoding/base64"
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/serializers"
	"github.com/batchcorp/plumber/util"
)

func Decode(opts *cli.Options, msgDesc *desc.MessageDescriptor, message []byte) ([]byte, error) {
	if opts.ReadOutputType == "protobuf" {
		// SQS doesn't like binary
		if opts.AWSSQS.QueueName != "" {
			// Our implementation of 'protobuf-over-sqs' encodes protobuf in b64
			plain, err := base64.StdEncoding.DecodeString(string(message))
			if err != nil {
				return nil, fmt.Errorf("unable to decode base64 to protobuf")
			}
			message = plain
		}

		decoded, err := pb.DecodeProtobufToJSON(dynamic.NewMessage(msgDesc), message)
		if err != nil {
			if !opts.ReadFollow {
				return nil, fmt.Errorf("unable to decode protobuf message: %s", err)
			}

			printer.Error(fmt.Sprintf("unable to decode protobuf message: %s", err))
			return nil, err
		}

		message = decoded
	}

	// Handle AVRO
	if opts.AvroSchemaFile != "" {
		// SQS doesn't like binary
		if opts.AWSSQS.QueueName != "" {
			plain, err := base64.StdEncoding.DecodeString(string(message))
			if err != nil {
				return nil, fmt.Errorf("unable to decode base64 to protobuf")
			}
			message = plain
		}

		decoded, err := serializers.AvroDecode(opts.AvroSchemaFile, message)
		if err != nil {
			printer.Error(fmt.Sprintf("unable to decode AVRO message: %s", err))
			return nil, err
		}
		message = decoded
	}

	data := make([]byte, 0)

	var convertErr error

	switch opts.ReadConvert {
	case "base64":
		data, convertErr = base64.StdEncoding.DecodeString(string(message))
	case "gzip":
		data, convertErr = util.Gunzip(message)
	default:
		data = message
	}

	if convertErr != nil {
		if !opts.ReadFollow {
			return nil, errors.Wrap(convertErr, "unable to complete conversion")
		}

		printer.Error(fmt.Sprintf("unable to complete conversion for message: %s", convertErr))
		return message, nil
	}

	return data, nil
}
