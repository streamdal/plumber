package codegen

import (
	"github.com/v2pro/wombat/generic"
)

func init() {
	decodeAnything.ImportFunc(decodeStruct)
}

var decodeStruct = generic.DefineFunc(
	"DecodeStruct(dst DT, src ST)").
	Param("EXT", "user provided extension").
	Param("DT", "the dst type to copy into").
	Param("ST", "the src type to copy from").
	ImportFunc(decodeAnything).
	Generators(
	"calcBindings", calcBindings,
	"assignDecode", func(binding map[string]interface{}, decodeFuncName string) string {
		binding["decode"] = decodeFuncName
		return ""
	}).
	Source(`
{{ $bindings := calcBindings (.DT|elem) }}
{{ range $_, $binding := $bindings}}
	{{ $decode := expand "DecodeAnything" "EXT" $.EXT "DT" $binding.fieldType "ST" $.ST }}
	{{ assignDecode $binding $decode }}
{{ end }}
src.ReadStructHeader()
for {
	fieldType, fieldId := src.ReadStructField()
	if fieldType == 0 {
		return
	}
	switch fieldId {
		{{ range $_, $binding := $bindings }}
			case {{ $binding.fieldId }}:
				{{$binding.decode}}(&dst.{{$binding.fieldName}}, src)
		{{ end }}
		default:
			src.Discard(fieldType)
	}
}`)