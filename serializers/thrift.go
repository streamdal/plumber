package serializers

import (
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/batchcorp/thrifty"
)

// DecodeThrift decodes a thrift message with the given structName utilizing .thrift IDL files found in dirs
func DecodeThrift(dirs []string, structName string, message []byte) ([]byte, error) {
	// Still allow pre-thrifty library decoding if the user didn't specify thrift flags
	if len(dirs) == 0 && structName == "" {
		decoded, err := thrifty.DecodeWithoutIDL(message)
		if err != nil {
			return nil, err
		}

		return decoded, nil
	}

	// At least one of schema/struct name provided. Use thrifty lib for decoding with IDL

	if len(dirs) == 0 {
		return nil, errors.New("--thrift-dirs cannot be empty")
	}
	if structName == "" {
		return nil, errors.New("--struct-name cannot be empty")
	}

	idlFiles, err := readThriftDirs(dirs)
	if err != nil {
		return nil, err
	}

	idl, err := thrifty.ParseIDLFiles(idlFiles)
	if err != nil {
		return nil, err
	}

	decoded, err := thrifty.DecodeWithParsedIDL(idl, message, structName)
	if err != nil {
		return nil, err
	}

	return decoded, nil
}

func readThriftDirs(dirs []string) (map[string][]byte, error) {
	idlFiles := make(map[string][]byte)

	// Read dirs into map
	for _, dir := range dirs {
		thriftFiles, err := filepath.Glob(filepath.Clean(dir) + "/" + "*.thrift")
		if err != nil {
			return nil, errors.Wrapf(err, "unable to find thrift files in dir '%s'", dir)
		}

		for _, file := range thriftFiles {
			data, err := os.ReadFile(file)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to read file '%s'", file)
			}

			idlFiles[file] = data
		}
	}

	return idlFiles, nil
}
