package reflection

import (
	"github.com/thrift-iterator/go/protocol"
	"github.com/thrift-iterator/go/spi"
	"reflect"
	"unsafe"
)

func EncoderOf(extension spi.Extension, valType reflect.Type) spi.ValEncoder {
	isPtr := valType.Kind() == reflect.Ptr
	isOnePtrArray := valType.Kind() == reflect.Array && valType.Len() == 1 &&
		valType.Elem().Kind() == reflect.Ptr
	isOnePtrStruct := valType.Kind() == reflect.Struct && valType.NumField() == 1 &&
		valType.Field(0).Type.Kind() == reflect.Ptr
	isOneMapStruct := valType.Kind() == reflect.Struct && valType.NumField() == 1 &&
		valType.Field(0).Type.Kind() == reflect.Map
	if isPtr || isOnePtrArray || isOnePtrStruct || isOneMapStruct {
		return &ptrEncoderAdapter{encoderOf(extension, "", valType)}
	}
	return &valEncoderAdapter{encoderOf(extension, "", valType)}
}

func encoderOf(extension spi.Extension, prefix string, valType reflect.Type) internalEncoder {
	extEncoder := extension.EncoderOf(valType)
	if extEncoder != nil {
		valObj := reflect.New(valType).Elem().Interface()
		valEmptyInterface := *(*emptyInterface)(unsafe.Pointer(&valObj))
		return &internalEncoderAdapter{valEmptyInterface: valEmptyInterface, encoder: extEncoder}
	}
	if byteSliceType == valType {
		return &binaryEncoder{}
	}
	if isEnumType(valType) {
		return &int32Encoder{}
	}
	switch valType.Kind() {
	case reflect.String:
		return &stringEncoder{}
	case reflect.Bool:
		return &boolEncoder{}
	case reflect.Int8:
		return &int8Encoder{}
	case reflect.Uint8:
		return &uint8Encoder{}
	case reflect.Int16:
		return &int16Encoder{}
	case reflect.Uint16:
		return &uint16Encoder{}
	case reflect.Int32:
		return &int32Encoder{}
	case reflect.Uint32:
		return &uint32Encoder{}
	case reflect.Int64:
		return &int64Encoder{}
	case reflect.Uint64:
		return &uint64Encoder{}
	case reflect.Int:
		return &intEncoder{}
	case reflect.Uint:
		return &uintEncoder{}
	case reflect.Float32:
		return &float32Encoder{}
	case reflect.Float64:
		return &float64Encoder{}
	case reflect.Slice:
		return &sliceEncoder{
			sliceType:   valType,
			elemType:    valType.Elem(),
			elemEncoder: encoderOf(extension, prefix+" [sliceElem]", valType.Elem()),
		}
	case reflect.Map:
		sampleObj := reflect.New(valType).Elem().Interface()
		elemType := valType.Elem()
		if elemType.Kind() == reflect.Ptr {
			elemType = elemType.Elem()
		}
		return &mapEncoder{
			keyEncoder:   encoderOf(extension, prefix+" [mapKey]", valType.Key()),
			elemEncoder:  encoderOf(extension, prefix+" [mapElem]", elemType),
			mapInterface: *(*emptyInterface)(unsafe.Pointer(&sampleObj)),
		}
	case reflect.Struct:
		encoderFields := make([]structEncoderField, 0, valType.NumField())
		for i := 0; i < valType.NumField(); i++ {
			refField := valType.Field(i)
			fieldId := parseFieldId(refField)
			if fieldId == -1 {
				continue
			}
			encoderField := structEncoderField{
				offset:  refField.Offset,
				fieldId: fieldId,
				encoder: encoderOf(extension, prefix+" "+refField.Name, refField.Type),
			}
			encoderFields = append(encoderFields, encoderField)
		}
		return &structEncoder{
			fields: encoderFields,
		}
	case reflect.Ptr:
		return &pointerEncoder{
			valType:    valType.Elem(),
			valEncoder: encoderOf(extension, prefix+" [ptrElem]", valType.Elem()),
		}
	}
	return &unknownEncoder{prefix, valType}
}

type unknownEncoder struct {
	prefix  string
	valType reflect.Type
}

func (encoder *unknownEncoder) encode(ptr unsafe.Pointer, stream spi.Stream) {
	stream.ReportError("decode "+encoder.prefix, "do not know how to encode "+encoder.valType.String())
}

func (encoder *unknownEncoder) thriftType() protocol.TType {
	return protocol.TypeStop
}
