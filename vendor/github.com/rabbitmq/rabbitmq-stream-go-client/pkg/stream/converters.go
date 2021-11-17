package stream

import (
	"fmt"
	"github.com/pkg/errors"
	"regexp"
	"strconv"
	"strings"
)

const (
	UnitMb              = "mb"
	UnitKb              = "kb"
	UnitGb              = "gb"
	UnitTb              = "tb"
	kilobytesMultiplier = 1000
	megabytesMultiplier = 1000 * 1000
	gigabytesMultiplier = 1000 * 1000 * 1000
	terabytesMultiplier = 1000 * 1000 * 1000 * 1000
)

type ByteCapacity struct {
	bytes int64
	error error
}

func (byteCapacity ByteCapacity) B(value int64) *ByteCapacity {
	return &ByteCapacity{bytes: value, error: nil}
}

func (byteCapacity ByteCapacity) KB(value int64) *ByteCapacity {
	return &ByteCapacity{bytes: value * kilobytesMultiplier, error: nil}
}

func (byteCapacity ByteCapacity) MB(value int64) *ByteCapacity {
	return &ByteCapacity{bytes: value * megabytesMultiplier, error: nil}
}

func (byteCapacity ByteCapacity) GB(value int64) *ByteCapacity {
	return &ByteCapacity{bytes: value * gigabytesMultiplier, error: nil}
}
func (byteCapacity ByteCapacity) TB(value int64) *ByteCapacity {
	return &ByteCapacity{bytes: value * terabytesMultiplier, error: nil}
}

func (byteCapacity ByteCapacity) From(value string) *ByteCapacity {
	if value == "" || value == "0" {
		return &ByteCapacity{bytes: 0, error: nil}
	}

	match, err := regexp.Compile("^((kb|mb|gb|tb))")
	if err != nil {
		return &ByteCapacity{bytes: 0,
			error: errors.New(fmt.Sprintf("Capacity, invalid unit size format:%s", value))}
	}

	foundUnitSize := strings.ToLower(value[len(value)-2:])

	if match.MatchString(foundUnitSize) {

		size, err := strconv.Atoi(value[:len(value)-2])
		if err != nil {
			return &ByteCapacity{bytes: 0, error: errors.New(fmt.Sprintf("Capacity, Invalid number format: %s", value))}
		}

		switch foundUnitSize {
		case UnitKb:
			return byteCapacity.KB(int64(size))

		case UnitMb:
			return byteCapacity.MB(int64(size))

		case UnitGb:
			return byteCapacity.GB(int64(size))

		case UnitTb:
			return byteCapacity.TB(int64(size))
		}

	}

	return &ByteCapacity{bytes: 0,
		error: errors.New(fmt.Sprintf("Capacity, Invalid unit size format: %s", value))}

}
