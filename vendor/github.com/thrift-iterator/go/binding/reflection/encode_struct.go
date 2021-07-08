package reflection

import (
	"github.com/thrift-iterator/go/protocol"
	"github.com/thrift-iterator/go/spi"
	"unsafe"
)

type structEncoder struct {
	fields []structEncoderField
}

type structEncoderField struct {
	offset  uintptr
	fieldId protocol.FieldId
	encoder internalEncoder
}

func (encoder *structEncoder) encode(ptr unsafe.Pointer, stream spi.Stream) {
	stream.WriteStructHeader()
	for _, field := range encoder.fields {
		fieldPtr := unsafe.Pointer(uintptr(ptr) + field.offset)
		switch field.encoder.(type) {
		case *pointerEncoder, *sliceEncoder:
			if *(*unsafe.Pointer)(fieldPtr) == nil {
				continue
			}
		case *mapEncoder:
			if *(*unsafe.Pointer)(fieldPtr) == nil {
				continue
			}
			fieldPtr = *(*unsafe.Pointer)(fieldPtr)
		}
		stream.WriteStructField(field.encoder.thriftType(), field.fieldId)
		field.encoder.encode(fieldPtr, stream)
	}
	stream.WriteStructFieldStop()
}

func (encoder *structEncoder) thriftType() protocol.TType {
	return protocol.TypeStruct
}