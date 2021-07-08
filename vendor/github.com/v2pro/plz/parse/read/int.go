package read

import (
	"strconv"
	"errors"
	"math"
	"github.com/v2pro/plz/parse"
)

var intDigits []int8

const invalidCharForNumber = int8(-1)
const uint64SafeToMultiple10 = uint64(0xffffffffffffffff)/10 - 1

func init() {
	intDigits = make([]int8, 256)
	for i := 0; i < len(intDigits); i++ {
		intDigits[i] = invalidCharForNumber
	}
	for i := int8('0'); i <= int8('9'); i++ {
		intDigits[i] = i - int8('0')
	}
}

func Int(src *parse.Source) int {
	if strconv.IntSize == 32 {
		return int(Int32(src))
	}
	return int(Int64(src))
}

func Int32(src *parse.Source) int32 {
	c := src.Peek()[0]
	if c == '-' {
		src.ConsumeN(1)
		val := Uint64(src)
		if val > math.MaxInt32+1 {
			src.ReportError(errors.New("int32: overflow"))
			return 0
		}
		return -int32(val)
	}
	val := Uint64(src)
	if val > math.MaxInt32 {
		src.ReportError(errors.New("int32: overflow"))
		return 0
	}
	return int32(val)
}

func Int64(src *parse.Source) int64 {
	c := src.Peek()[0]
	if c == '-' {
		src.ConsumeN(1)
		val := Uint64(src)
		if val > math.MaxInt64+1 {
			src.ReportError(errors.New("int64: overflow"))
			return 0
		}
		return -int64(val)
	}
	val := Uint64(src)
	if val > math.MaxInt64 {
		src.ReportError(errors.New("int64: overflow"))
		return 0
	}
	return int64(val)
}

func Uint64(src *parse.Source) uint64 {
	value := uint64(0)
	for src.Error() == nil {
		buf := src.Peek()
		for i, c := range buf {
			ind := intDigits[c]
			if ind == invalidCharForNumber {
				src.ConsumeN(i)
				return value
			}
			if value > uint64SafeToMultiple10 {
				value2 := (value << 3) + (value << 1) + uint64(ind)
				if value2 < value {
					src.ReportError(errors.New("uint64: overflow"))
					return 0
				}
				value = value2
				continue
			}
			value = (value << 3) + (value << 1) + uint64(ind)
		}
		src.Consume()
	}
	return value
}
