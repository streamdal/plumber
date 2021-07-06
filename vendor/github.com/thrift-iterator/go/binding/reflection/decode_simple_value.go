package reflection

import (
	"github.com/thrift-iterator/go/spi"
	"unsafe"
)

type binaryDecoder struct {
}

func (decoder *binaryDecoder) decode(ptr unsafe.Pointer, iter spi.Iterator) {
	*(*[]byte)(ptr) = iter.ReadBinary()
}

type boolDecoder struct {
}

func (decoder *boolDecoder) decode(ptr unsafe.Pointer, iter spi.Iterator) {
	*(*bool)(ptr) = iter.ReadBool()
}

type float64Decoder struct {
}

func (decoder *float64Decoder) decode(ptr unsafe.Pointer, iter spi.Iterator) {
	*(*float64)(ptr) = iter.ReadFloat64()
}

type int8Decoder struct {
}

func (decoder *int8Decoder) decode(ptr unsafe.Pointer, iter spi.Iterator) {
	*(*int8)(ptr) = iter.ReadInt8()
}

type uint8Decoder struct {
}

func (decoder *uint8Decoder) decode(ptr unsafe.Pointer, iter spi.Iterator) {
	*(*uint8)(ptr) = iter.ReadUint8()
}

type int16Decoder struct {
}

func (decoder *int16Decoder) decode(ptr unsafe.Pointer, iter spi.Iterator) {
	*(*int16)(ptr) = iter.ReadInt16()
}

type uint16Decoder struct {
}

func (decoder *uint16Decoder) decode(ptr unsafe.Pointer, iter spi.Iterator) {
	*(*uint16)(ptr) = iter.ReadUint16()
}

type int32Decoder struct {
}

func (decoder *int32Decoder) decode(ptr unsafe.Pointer, iter spi.Iterator) {
	*(*int32)(ptr) = iter.ReadInt32()
}

type uint32Decoder struct {
}

func (decoder *uint32Decoder) decode(ptr unsafe.Pointer, iter spi.Iterator) {
	*(*uint32)(ptr) = iter.ReadUint32()
}

type int64Decoder struct {
}

func (decoder *int64Decoder) decode(ptr unsafe.Pointer, iter spi.Iterator) {
	*(*int64)(ptr) = iter.ReadInt64()
}

type uint64Decoder struct {
}

func (decoder *uint64Decoder) decode(ptr unsafe.Pointer, iter spi.Iterator) {
	*(*uint64)(ptr) = iter.ReadUint64()
}

type intDecoder struct {
}

func (decoder *intDecoder) decode(ptr unsafe.Pointer, iter spi.Iterator) {
	*(*int)(ptr) = iter.ReadInt()
}

type uintDecoder struct {
}

func (decoder *uintDecoder) decode(ptr unsafe.Pointer, iter spi.Iterator) {
	*(*uint)(ptr) = iter.ReadUint()
}

type stringDecoder struct {
}

func (decoder *stringDecoder) decode(ptr unsafe.Pointer, iter spi.Iterator) {
	*(*string)(ptr) = iter.ReadString()
}

