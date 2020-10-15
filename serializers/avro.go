package serializers

import (
	"fmt"
	"io/ioutil"

	"github.com/linkedin/goavro/v2"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/printer"
)

// AvroEncode takes in a path to a AVRO schema file, and plain json data
// and returns the binary encoded representation
func AvroEncode(avroSchemaPath string, data []byte) ([]byte, error) {

	avroSchema, readErr := ioutil.ReadFile(avroSchemaPath)
	if readErr != nil {
		return nil, fmt.Errorf("unable to read AVRO schema file '%s': %s", avroSchemaPath, readErr)
	}

	codec, err := goavro.NewCodec(string(avroSchema))
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

// AvroEncode takes in a path to a AVRO schema file, and binary encoded data
// and returns the plain JSON representation
func AvroDecode(avroSchemaPath string, data []byte) ([]byte, error) {
	avroSchema, readErr := ioutil.ReadFile(avroSchemaPath)
	if readErr != nil {
		return nil, fmt.Errorf("unable to read AVRO schema file '%s': %s", avroSchemaPath, readErr)
	}

	codec, err := goavro.NewCodec(string(avroSchema))
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
