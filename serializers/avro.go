package serializers

import (
	"archive/zip"
	"fmt"
	"io/ioutil"
	"strings"

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

// readZipFile reads the contents of a zip archive into a []byte
// TODO: remove duplication between this and pb.getProtoFilesFromZip()
func readZipFile(zf *zip.File) ([]byte, error) {
	f, err := zf.Open()
	if err != nil {
		return nil, errors.Wrap(err, "failed to open zip file")
	}
	defer f.Close()

	return ioutil.ReadAll(f)
}

// TruncateRepoArchiveDirectory strips the github repo zip file's base directory from a file map
func TruncateRepoArchiveDirectory(files map[string]string) map[string]string {
	cleaned := make(map[string]string)

	var zipBaseDir string

	for filePath, contents := range files {
		// Only need to do this once
		if zipBaseDir == "" {
			// Strip out zip base directory, which will look like "batchcorp-schemas-9789dfg70s980fdsfs"
			parts := strings.Split(filePath, "/")
			zipBaseDir = parts[0] + "/"
		}

		newPath := strings.Replace(filePath, zipBaseDir, "", 1)
		cleaned[newPath] = contents
	}

	return cleaned
}
