package reflection

import (
	"reflect"
	"github.com/thrift-iterator/go/spi"
	"unsafe"
	"github.com/thrift-iterator/go/protocol"
	"strings"
	"unicode"
	"strconv"
)

var byteSliceType = reflect.TypeOf(([]byte)(nil))

func DecoderOf(extension spi.Extension, valType reflect.Type) spi.ValDecoder {
	if valType.Kind() != reflect.Ptr {
		return &valDecoderAdapter{&unknownDecoder{
			prefix: "unmarshal into non-pointer type", valType: valType}}
	}
	return &valDecoderAdapter{decoderOf(extension, "", valType.Elem())}
}

func decoderOf(extension spi.Extension, prefix string, valType reflect.Type) internalDecoder {
	extDecoder := extension.DecoderOf(reflect.PtrTo(valType))
	if extDecoder != nil {
		valObj := reflect.New(valType).Interface()
		valEmptyInterface := *(*emptyInterface)(unsafe.Pointer(&valObj))
		return &internalDecoderAdapter{valEmptyInterface: valEmptyInterface, decoder: extDecoder}
	}
	if byteSliceType == valType {
		return &binaryDecoder{}
	}
	if isEnumType(valType) {
		return &int32Decoder{}
	}
	switch valType.Kind() {
	case reflect.Bool:
		return &boolDecoder{}
	case reflect.Float64:
		return &float64Decoder{}
	case reflect.Int:
		return &intDecoder{}
	case reflect.Uint:
		return &uintDecoder{}
	case reflect.Int8:
		return &int8Decoder{}
	case reflect.Uint8:
		return &uint8Decoder{}
	case reflect.Int16:
		return &int16Decoder{}
	case reflect.Uint16:
		return &uint16Decoder{}
	case reflect.Int32:
		return &int32Decoder{}
	case reflect.Uint32:
		return &uint32Decoder{}
	case reflect.Int64:
		return &int64Decoder{}
	case reflect.Uint64:
		return &uint64Decoder{}
	case reflect.String:
		return &stringDecoder{}
	case reflect.Ptr:
		return &pointerDecoder{
			valType: valType.Elem(),
			valDecoder: decoderOf(extension, prefix+" [ptrElem]", valType.Elem()),
		}
	case reflect.Slice:
		return &sliceDecoder{
			elemType:    valType.Elem(),
			sliceType:   valType,
			elemDecoder: decoderOf(extension, prefix+" [sliceElem]", valType.Elem()),
		}
	case reflect.Map:
		sampleObj := reflect.New(valType).Interface()
		return &mapDecoder{
			keyType:      valType.Key(),
			keyDecoder:   decoderOf(extension, prefix+" [mapKey]", valType.Key()),
			elemType:     valType.Elem(),
			elemDecoder:  decoderOf(extension, prefix+" [mapElem]", valType.Elem()),
			mapType:      valType,
			mapInterface: *(*emptyInterface)(unsafe.Pointer(&sampleObj)),
		}
	case reflect.Struct:
		decoderFields := make([]structDecoderField, 0, valType.NumField())
		decoderFieldMap := map[protocol.FieldId]structDecoderField{}
		for i := 0; i < valType.NumField(); i++ {
			refField := valType.Field(i)
			fieldId := parseFieldId(refField)
			if fieldId == -1 {
				continue
			}
			decoderField := structDecoderField{
				offset: refField.Offset,
				fieldId: fieldId,
				decoder: decoderOf(extension, prefix + " " + refField.Name, refField.Type),
			}
			decoderFields = append(decoderFields, decoderField)
			decoderFieldMap[fieldId] = decoderField
		}
		return &structDecoder{
			fields: decoderFields,
			fieldMap: decoderFieldMap,
		}
	}
	return &unknownDecoder{prefix, valType}
}

func isEnumType(valType reflect.Type) bool {
	if valType.Kind() != reflect.Int64 {
		return false
	}
	_, hasStringMethod := valType.MethodByName("String")
	return hasStringMethod
}

func parseFieldId(refField reflect.StructField) protocol.FieldId {
	if !unicode.IsUpper(rune(refField.Name[0])) {
		return -1
	}
	thriftTag := refField.Tag.Get("thrift")
	if thriftTag == "" {
		return -1
	}
	parts := strings.Split(thriftTag, ",")
	if len(parts) < 2 {
		return -1
	}
	fieldId, err := strconv.Atoi(parts[1])
	if err != nil {
		return -1
	}
	return protocol.FieldId(fieldId)
}

type unknownDecoder struct {
	prefix  string
	valType reflect.Type
}

func (decoder *unknownDecoder) decode(ptr unsafe.Pointer, iterator spi.Iterator) {
	iterator.ReportError("decode "+decoder.prefix, "do not know how to decode "+decoder.valType.String())
}
