package codegen

import (
	"reflect"
	"github.com/v2pro/wombat/generic"
	"github.com/batchcorp/thrift-iterator/protocol"
)

func dispatchEncode(extension *Extension, srcType reflect.Type) (string, protocol.TType) {
	extEncoder := extension.EncoderOf(srcType)
	if extEncoder != nil {
		extension.ExtTypes = append(extension.ExtTypes, srcType)
		return "EncodeByExtension", extEncoder.ThriftType()
	}
	if srcType == byteArrayType {
		return "EncodeBinary", protocol.TypeString
	}
	if isEnumType(srcType) {
		return "EncodeEnum", protocol.TypeI32
	}
	switch srcType.Kind() {
	case reflect.Slice:
		return "EncodeSlice", protocol.TypeList
	case reflect.Map:
		return "EncodeMap", protocol.TypeMap
	case reflect.Struct:
		return "EncodeStruct", protocol.TypeStruct
	case reflect.Ptr:
		_, ttype := dispatchEncode(extension, srcType.Elem())
		return "EncodePointer", ttype
	}
	return "EncodeSimpleValue", thriftTypeMap[srcType.Kind()]
}

func dispatchThriftType(extension *Extension, srcType reflect.Type) int {
	_, ttype := dispatchEncode(extension, srcType)
	return int(ttype)
}

var encodeAnything = generic.DefineFunc("EncodeAnything(dst DT, src ST)").
	Param("EXT", "user provided extension").
	Param("DT", "the dst type to copy into").
	Param("ST", "the src type to copy from").
	Generators(
		"dispatchEncode", func(extension *Extension, srcType reflect.Type) string {
			encode, _ := dispatchEncode(extension, srcType)
			return encode
		}).
	Source(`
{{ $tmpl := dispatchEncode .EXT .ST }}
{{ if eq $tmpl "EncodeByExtension" }}
	dst.GetEncoder("{{ .ST|name }}").Encode(src, dst)
{{ else }}
	{{ $encode := expand $tmpl "EXT" .EXT "DT" .DT "ST" .ST }}
	{{$encode}}(dst, src)
{{ end }}
`)
