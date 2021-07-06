package reflection

import (
	"unsafe"
	"github.com/thrift-iterator/go/spi"
	"github.com/thrift-iterator/go/protocol"
)

type binaryEncoder struct {
}

func (encoder *binaryEncoder) encode(ptr unsafe.Pointer, iter spi.Stream) {
	iter.WriteBinary(*(*[]byte)(ptr))
}

func (encoder *binaryEncoder) thriftType() protocol.TType {
	return protocol.TypeString
}

type stringEncoder struct {
}

func (encoder *stringEncoder) encode(ptr unsafe.Pointer, iter spi.Stream) {
	iter.WriteString(*(*string)(ptr))
}

func (encoder *stringEncoder) thriftType() protocol.TType {
	return protocol.TypeString
}

type boolEncoder struct {
}

func (encoder *boolEncoder) encode(ptr unsafe.Pointer, iter spi.Stream) {
	iter.WriteBool(*(*bool)(ptr))
}

func (encoder *boolEncoder) thriftType() protocol.TType {
	return protocol.TypeBool
}

type int8Encoder struct {
}

func (encoder *int8Encoder) encode(ptr unsafe.Pointer, iter spi.Stream) {
	iter.WriteInt8(*(*int8)(ptr))
}

func (encoder *int8Encoder) thriftType() protocol.TType {
	return protocol.TypeI08
}

type uint8Encoder struct {
}

func (encoder *uint8Encoder) encode(ptr unsafe.Pointer, iter spi.Stream) {
	iter.WriteUint8(*(*uint8)(ptr))
}

func (encoder *uint8Encoder) thriftType() protocol.TType {
	return protocol.TypeI08
}

type int16Encoder struct {
}

func (encoder *int16Encoder) encode(ptr unsafe.Pointer, iter spi.Stream) {
	iter.WriteInt16(*(*int16)(ptr))
}

func (encoder *int16Encoder) thriftType() protocol.TType {
	return protocol.TypeI16
}

type uint16Encoder struct {
}

func (encoder *uint16Encoder) encode(ptr unsafe.Pointer, iter spi.Stream) {
	iter.WriteUint16(*(*uint16)(ptr))
}

func (encoder *uint16Encoder) thriftType() protocol.TType {
	return protocol.TypeI16
}

type int32Encoder struct {
}

func (encoder *int32Encoder) encode(ptr unsafe.Pointer, iter spi.Stream) {
	iter.WriteInt32(*(*int32)(ptr))
}

func (encoder *int32Encoder) thriftType() protocol.TType {
	return protocol.TypeI32
}

type uint32Encoder struct {
}

func (encoder *uint32Encoder) encode(ptr unsafe.Pointer, iter spi.Stream) {
	iter.WriteUint32(*(*uint32)(ptr))
}

func (encoder *uint32Encoder) thriftType() protocol.TType {
	return protocol.TypeI32
}

type int64Encoder struct {
}

func (encoder *int64Encoder) encode(ptr unsafe.Pointer, iter spi.Stream) {
	iter.WriteInt64(*(*int64)(ptr))
}

func (encoder *int64Encoder) thriftType() protocol.TType {
	return protocol.TypeI64
}

type uint64Encoder struct {
}

func (encoder *uint64Encoder) encode(ptr unsafe.Pointer, iter spi.Stream) {
	iter.WriteUint64(*(*uint64)(ptr))
}

func (encoder *uint64Encoder) thriftType() protocol.TType {
	return protocol.TypeI64
}

type intEncoder struct {
}

func (encoder *intEncoder) encode(ptr unsafe.Pointer, iter spi.Stream) {
	iter.WriteInt(*(*int)(ptr))
}

func (encoder *intEncoder) thriftType() protocol.TType {
	return protocol.TypeI64
}

type uintEncoder struct {
}

func (encoder *uintEncoder) encode(ptr unsafe.Pointer, iter spi.Stream) {
	iter.WriteUint(*(*uint)(ptr))
}

func (encoder *uintEncoder) thriftType() protocol.TType {
	return protocol.TypeI64
}

type float64Encoder struct {
}

func (encoder *float64Encoder) encode(ptr unsafe.Pointer, iter spi.Stream) {
	iter.WriteFloat64(*(*float64)(ptr))
}

func (encoder *float64Encoder) thriftType() protocol.TType {
	return protocol.TypeDouble
}

type float32Encoder struct {
}

func (encoder *float32Encoder) encode(ptr unsafe.Pointer, iter spi.Stream) {
	iter.WriteFloat64(float64(*(*float32)(ptr)))
}

func (encoder *float32Encoder) thriftType() protocol.TType {
	return protocol.TypeDouble
}
