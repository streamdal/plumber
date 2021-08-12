package util

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/batchcorp/plumber/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Gunzip decompresses a slice of bytes and returns a slice of decompressed
// bytes or an error.
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

func DirsExist(dirs []string) error {
	var errs []string

	for _, dir := range dirs {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			errs = append(errs, fmt.Sprintf("'%s' does not exist", dir))
		}
	}

	if errs == nil {
		return nil
	}

	return errors.New(strings.Join(errs, "; "))
}

func GetBackendName(cmd string) (string, error) {
	splitCmd := strings.Split(cmd, " ")

	if len(splitCmd) < 2 {
		return "", errors.New("unexpected number of results in split command")
	}

	return splitCmd[1], nil
}

// WriteError is a wrapper for logging an error + writing to an error channel.
// Both the logger and error channel can be nil.
func WriteError(l *logrus.Entry, errorCh chan *types.ErrorMessage, err error) {
	if l != nil {
		l.Error(err)
	}

	if errorCh != nil {
		go func() {
			errorCh <- &types.ErrorMessage{
				OccurredAt: time.Now().UTC(),
				Error:      err,
			}
		}()
	}
}
