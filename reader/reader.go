package reader

import (
	"encoding/base64"

	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	thrifter "github.com/thrift-iterator/go"
	"github.com/thrift-iterator/go/general"

	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/serializers"
	"github.com/batchcorp/plumber/util"
)

// Decode will attempt to decode the payload IF decode options are present
func Decode(readOpts *opts.ReadOptions, fds *dpb.FileDescriptorSet, message []byte) ([]byte, error) {
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
		decoded, err := pb.DecodeProtobufToJSON(readOpts, fds, message)
		if err != nil {
			return nil, errors.Wrap(err, "unable to decode protobuf message")
		}

		message = decoded
	}

	// Avro
	if readOpts.DecodeOptions.DecodeType == encoding.DecodeType_DECODE_TYPE_AVRO {
		// SQS doesn't like binary
		if readOpts.AwsSqs != nil && readOpts.AwsSqs.Args.QueueName != "" {
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
