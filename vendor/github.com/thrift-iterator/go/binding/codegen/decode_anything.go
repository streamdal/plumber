package codegen

import (
	"github.com/v2pro/wombat/generic"
	"reflect"
)

func dispatchDecode(extension *Extension, dstType reflect.Type) string {
	if extension.DecoderOf(dstType) != nil {
		extension.ExtTypes = append(extension.ExtTypes, dstType)
		return "DecodeByExtension"
	}
	if dstType.Kind() != reflect.Ptr {
		panic("can only decode into pointer")
	}
	dstType = dstType.Elem()
	if dstType == byteArrayType {
		return "DecodeBinary"
	}
	if isEnumType(dstType) {
		return "DecodeEnum"
	}
	switch dstType.Kind() {
	case reflect.Slice:
		return "DecodeSlice"
	case reflect.Map:
		return "DecodeMap"
	case reflect.Struct:
		return "DecodeStruct"
	case reflect.Ptr:
		return "DecodePointer"
	}
	if _, isSimpleValue := simpleValueMap[dstType.Kind()]; isSimpleValue {
		return "DecodeSimpleValue"
	}
	panic("unsupported type")
}

var decodeAnything = generic.DefineFunc("DecodeAnything(dst DT, src ST)").
	Param("EXT", "user provided extension").
	Param("DT", "the dst type to copy into").
	Param("ST", "the src type to copy from").
	Generators("dispatchDecode", dispatchDecode).
	Source(`
{{ $tmpl := dispatchDecode .EXT .DT }}
{{ if eq $tmpl "DecodeByExtension" }}
	src.GetDecoder("{{ .DT|name }}").Decode(dst, src)
{{ else }}
	{{ $decode := expand $tmpl "EXT" .EXT "DT" .DT "ST" .ST }}
	{{$decode}}(dst, src)
{{ end }}
`)
