package util

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func DurationSec(durationSec interface{}) time.Duration {
	if v, ok := durationSec.(int32); ok {
		return time.Duration(v) * time.Second
	}

	if v, ok := durationSec.(uint32); ok {
		return time.Duration(v) * time.Second
	}

	if v, ok := durationSec.(int64); ok {
		return time.Duration(v) * time.Second
	}

	if v, ok := durationSec.(int); ok {
		return time.Duration(v) * time.Second
	}

	return 0
}

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

// WriteError is a wrapper for logging an error + writing to an error channel.
// Both the logger and error channel can be nil.
func WriteError(l *logrus.Entry, errorCh chan *records.ErrorRecord, err error) {
	if l != nil {
		l.Error(err)
	}

	if errorCh != nil {
		go func() {
			errorCh <- &records.ErrorRecord{
				OccurredAtUnixTsUtc: time.Now().UTC().UnixNano(),
				Error:               err.Error(),
			}
		}()
	}
}
