package reflection

import (
	"unsafe"
	"github.com/batchcorp/thrift-iterator/spi"
	"github.com/batchcorp/thrift-iterator/protocol"
)

type internalDecoder interface {
	decode(ptr unsafe.Pointer, iter spi.Iterator)
}

type valDecoderAdapter struct {
	decoder internalDecoder
}

func (decoder *valDecoderAdapter) Decode(val interface{}, iter spi.Iterator) {
	ptr := (*emptyInterface)(unsafe.Pointer(&val)).word
	decoder.decoder.decode(ptr, iter)
}

type internalDecoderAdapter struct {
	decoder           spi.ValDecoder
	valEmptyInterface emptyInterface
}

func (decoder *internalDecoderAdapter) decode(ptr unsafe.Pointer, iter spi.Iterator) {
	valEmptyInterface := decoder.valEmptyInterface
	valEmptyInterface.word = ptr
	valObj := *(*interface{})((unsafe.Pointer(&valEmptyInterface)))
	decoder.decoder.Decode(valObj, iter)
}

type internalEncoder interface {
	encode(ptr unsafe.Pointer, stream spi.Stream)
	thriftType() protocol.TType
}

type valEncoderAdapter struct {
	encoder internalEncoder
}

func (encoder *valEncoderAdapter) Encode(val interface{}, stream spi.Stream) {
	ptr := (*emptyInterface)(unsafe.Pointer(&val)).word
	encoder.encoder.encode(ptr, stream)
}

func (encoder *valEncoderAdapter) ThriftType() protocol.TType {
	return encoder.encoder.thriftType()
}

type ptrEncoderAdapter struct {
	encoder internalEncoder
}

func (encoder *ptrEncoderAdapter) Encode(val interface{}, stream spi.Stream) {
	ptr := (*emptyInterface)(unsafe.Pointer(&val)).word
	encoder.encoder.encode(unsafe.Pointer(&ptr), stream)
}

func (encoder *ptrEncoderAdapter) ThriftType() protocol.TType {
	return encoder.encoder.thriftType()
}

type internalEncoderAdapter struct {
	encoder           spi.ValEncoder
	valEmptyInterface emptyInterface
}

func (encoder *internalEncoderAdapter) encode(ptr unsafe.Pointer, stream spi.Stream) {
	valEmptyInterface := encoder.valEmptyInterface
	valEmptyInterface.word = ptr
	valObj := *(*interface{})((unsafe.Pointer(&valEmptyInterface)))
	encoder.encoder.Encode(valObj, stream)
}

func (encoder *internalEncoderAdapter) thriftType() protocol.TType {
	return encoder.encoder.ThriftType()
}

// emptyInterface is the header for an interface{} value.
type emptyInterface struct {
	typ  unsafe.Pointer
	word unsafe.Pointer
}

// sliceHeader is a safe version of SliceHeader used within this package.
type sliceHeader struct {
	Data unsafe.Pointer
	Len  int
	Cap  int
}
