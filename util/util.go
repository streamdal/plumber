package util

import (
	"bytes"
	"compress/gzip"
	"io"

	"github.com/pkg/errors"
)

func Gunzip(data []byte) ([]byte, error) {
	b := bytes.NewBuffer(data)

	var r io.Reader

	r, err := gzip.NewReader(b)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new reader")
	}

	var resB bytes.Buffer

	if _, err := resB.ReadFrom(r); err != nil {
		return nil, errors.Wrap(err, "unable to read data from reader")
	}

	return resB.Bytes(), nil
}
