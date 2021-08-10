package reader

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	jsoniter "github.com/json-iterator/go"
	thrifter "github.com/thrift-iterator/go"
	"github.com/thrift-iterator/go/general"

	"github.com/hokaccha/go-prettyjson"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/serializers"
	"github.com/batchcorp/plumber/util"
)

func Decode(opts *options.Options, msgDesc *desc.MessageDescriptor, message []byte) ([]byte, error) {
	if opts.Decoding.ProtobufRootMessage != "" {
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
			if !opts.Read.Follow {
				return nil, fmt.Errorf("unable to decode protobuf message: %s", err)
			}

			printer.Error(fmt.Sprintf("unable to decode protobuf message: %s", err))
			return nil, err
		}

		message = decoded
	}

	// Handle AVRO
	if opts.Decoding.AvroSchemaFile != "" {
		// SQS doesn't like binary
		if opts.AWSSQS.QueueName != "" {
			plain, err := base64.StdEncoding.DecodeString(string(message))
			if err != nil {
				return nil, fmt.Errorf("unable to decode base64 to protobuf")
			}
			message = plain
		}

		decoded, err := serializers.AvroDecodeWithSchemaFile(opts.Decoding.AvroSchemaFile, message)
		if err != nil {
			printer.Error(fmt.Sprintf("unable to decode AVRO message: %s", err))
			return nil, err
		}
		message = decoded
	}

	if opts.Decoding.ThriftOutput {
		decoded, err := decodeThrift(message)
		if err != nil {
			printer.Error(fmt.Sprintf("unable to decode Thrift message: %s", err))
			return nil, err
		}

		message = decoded
	}

	var data []byte

	var convertErr error

	switch opts.Read.Convert {
	case "base64":
		data, convertErr = base64.StdEncoding.DecodeString(string(message))
	case "gzip":
		data, convertErr = util.Gunzip(message)
	default:
		data = message
	}

	if convertErr != nil {
		if !opts.Read.Follow {
			return nil, errors.Wrap(convertErr, "unable to complete conversion")
		}

		printer.Error(fmt.Sprintf("unable to complete conversion for message: %s", convertErr))
		return message, nil
	}

	if opts.Decoding.JSONOutput {
		if json.Valid(data) {
			colorized, err := prettyjson.Format(data)
			if err != nil {
				printer.Error(fmt.Sprintf("unable to colorize JSON output: %s", err))
			} else {
				data = colorized
			}
		}
	}

	return data, nil
}

// decodeThrift decodes a thrift encoded message
func decodeThrift(message []byte) ([]byte, error) {
	var obj general.Struct

	err := thrifter.Unmarshal(message, &obj)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read thrift message")
	}

	// jsoniter is needed to marshal map[interface{}]interface{} types
	js, err := jsoniter.Marshal(obj)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal thrift message to json")
	}
	return js, nil
}
