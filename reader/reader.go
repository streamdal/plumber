package reader

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/jhump/protoreflect/desc"
	jsoniter "github.com/json-iterator/go"
	thrifter "github.com/thrift-iterator/go"
	"github.com/thrift-iterator/go/general"

	"github.com/hokaccha/go-prettyjson"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/serializers"
	"github.com/batchcorp/plumber/util"
)

// Decode will attempt to decode the payload IF decode options are present
func Decode(readOpts *opts.ReadOptions, md *desc.MessageDescriptor, message []byte) ([]byte, error) {
	if readOpts == nil {
		return nil, errors.New("read options cannot be nil")
	}

	// If decode options are not provided, instantiate an empty decode options
	// so that we can avoid a panic & get to the last in the func.
	if readOpts.DecodeOptions == nil {
		readOpts.DecodeOptions = &encoding.DecodeOptions{}
	}

	// Protobuf
	if readOpts.DecodeOptions.DecodeType == encoding.DecodeType_DECODE_TYPE_PROTOBUF {
		if md == nil {
			return nil, errors.New("md cannot be nil if decode type is protobuf")
		}

		// SQS doesn't like binary
		if readOpts.Awssqs.Args.QueueName != "" {
			// Our implementation of 'protobuf-over-sqs' encodes protobuf in b64
			plain, err := base64.StdEncoding.DecodeString(string(message))
			if err != nil {
				return nil, fmt.Errorf("unable to decode base64 to protobuf")
			}
			message = plain
		}

		decoded, err := pb.DecodeProtobufToJSON(dynamic.NewMessage(md), message)
		if err != nil {
			return nil, errors.Wrap(err, "unable to decode protobuf message")
		}

		message = decoded
	}

	// Avro
	if readOpts.DecodeOptions.DecodeType == encoding.DecodeType_DECODE_TYPE_AVRO {
		// SQS doesn't like binary
		if readOpts.Awssqs.Args.QueueName != "" {
			plain, err := base64.StdEncoding.DecodeString(string(message))
			if err != nil {
				return nil, errors.Wrap(err, "unable to decode base64 to protobuf")
			}
			message = plain
		}

		decoded, err := serializers.AvroDecodeWithSchemaFile(readOpts.DecodeOptions.AvroSettings.AvroSchemaFile, message)
		if err != nil {
			return nil, errors.Wrap(err, "unable to decode avro message")
		}
		message = decoded
	}

	// Thrift
	if readOpts.DecodeOptions.DecodeType == encoding.DecodeType_DECODE_TYPE_THRIFT {
		decoded, err := decodeThrift(message)
		if err != nil {
			return nil, errors.Wrap(err, "unable to decode Thrift message")
		}

		message = decoded
	}

	// Output conversion

	var data []byte

	var convertErr error

	switch readOpts.ConvertOutput {
	case opts.ConvertOption_CONVERT_OPTION_BASE64:
		data, convertErr = base64.StdEncoding.DecodeString(string(message))
	case opts.ConvertOption_CONVERT_OPTION_GZIP:
		data, convertErr = util.Gunzip(message)
	default:
		data = message
	}

	if convertErr != nil {
		return nil, errors.Wrap(convertErr, "unable to complete conversion")
	}

	// Pretty output

	if readOpts.XCliOptions.Pretty {
		// Only do pretty output for JSON (for now)
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
