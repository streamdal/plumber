package reflection

import (
	"github.com/thrift-iterator/go/protocol"
	"unsafe"
	"github.com/thrift-iterator/go/spi"
)

type structDecoder struct {
	fields []structDecoderField
	fieldMap map[protocol.FieldId]structDecoderField
}

type structDecoderField struct {
	offset  uintptr
	fieldId protocol.FieldId
	decoder internalDecoder
}

func (decoder *structDecoder) decode(ptr unsafe.Pointer, iter spi.Iterator) {
	iter.ReadStructHeader()
	for _, field := range decoder.fields {
		fieldType, fieldId := iter.ReadStructField()
		if field.fieldId == fieldId {
			field.decoder.decode(unsafe.Pointer(uintptr(ptr) + field.offset), iter)
		} else {
			decoder.decodeByMap(ptr, iter, fieldType, fieldId)
			return
		}
	}
	fieldType, fieldId := iter.ReadStructField()
	decoder.decodeByMap(ptr, iter, fieldType, fieldId)
}

func (decoder *structDecoder) decodeByMap(ptr unsafe.Pointer, iter spi.Iterator,
	fieldType protocol.TType, fieldId protocol.FieldId) {
	for {
		if protocol.TypeStop == fieldType {
			return
		}
		field, isFound := decoder.fieldMap[fieldId]
		if isFound {
			field.decoder.decode(unsafe.Pointer(uintptr(ptr) + field.offset), iter)
		} else {
			iter.Discard(fieldType)
		}
		fieldType, fieldId = iter.ReadStructField()
	}
}