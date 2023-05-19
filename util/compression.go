package util

import (
	"bytes"
	"compress/gzip"

	"github.com/pkg/errors"
)

// Compress data using gzip. Used before uploading WASM files to plumber server
func Compress(data []byte) ([]byte, error) {
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)

	if _, err := gz.Write(data); err != nil {
		return nil, errors.Wrap(err, "unable to write to gzip writer")
	}

	if err := gz.Flush(); err != nil {
		return nil, errors.Wrap(err, "unable to flush gzip writer")
	}

	if err := gz.Close(); err != nil {
		return nil, errors.Wrap(err, "unable to close gzip writer")
	}

	return b.Bytes(), nil
}

// Decompress data using gzip. Used after downloading WASM files from plumber server
func Decompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewBuffer(data))
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new gzip reader")
	}

	var decompressed bytes.Buffer
	_, err = decompressed.ReadFrom(r)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read from gzip reader")
	}

	return decompressed.Bytes(), nil
}
