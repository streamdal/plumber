package serializers

import (
	"fmt"
	"io/ioutil"

	"github.com/linkedin/goavro/v2"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/printer"
)

func AvroEncode(schema, data []byte) ([]byte, error) {
	codec, err := goavro.NewCodec(string(schema))
	if err != nil {
		return nil, errors.Wrap(err, "unable to read AVRO schema")
	}

	// Convert from text to native
	native, _, err := codec.NativeFromTextual(data)
	if err != nil {
		return nil, errors.Wrap(err, "unable to apply AVRO schema to input data")
	}

	// Convert native to binary before shipping out
	binary, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, errors.Wrap(err, "unable to convert input data")
	}

	return binary, nil
}

// AvroDecode takes in a path to a AVRO schema file, and binary encoded data
// and returns the plain JSON representation
func AvroDecode(schema, data []byte) ([]byte, error) {
	codec, err := goavro.NewCodec(string(schema))
	if err != nil {
		return nil, errors.Wrap(err, "unable to read AVRO schema")
	}

	native, _, err := codec.NativeFromBinary(data)
	if err != nil {
		printer.Error(fmt.Sprintf("unable to decode AVRO message: %s", err.Error()))
		return nil, err
	}

	return codec.TextualFromNative(nil, native)
}

func AvroEncodeWithSchemaFile(schemaPath string, data []byte) ([]byte, error) {
	schema, readErr := ioutil.ReadFile(schemaPath)
	if readErr != nil {
		return nil, fmt.Errorf("unable to read AVRO schema file '%s': %s", schemaPath, readErr)
	}

	return AvroEncode(schema, data)
}

func AvroDecodeWithSchemaFile(schemaPath string, data []byte) ([]byte, error) {
	if schemaPath == "" {
		return data, nil
	}
	schema, readErr := ioutil.ReadFile(schemaPath)
	if readErr != nil {
		return nil, fmt.Errorf("unable to read AVRO schema file '%s': %s", schemaPath, readErr)
	}

	return AvroDecode(schema, data)
}
