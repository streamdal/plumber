package codegen

import (
	"github.com/v2pro/wombat/generic"
)

var Decode = generic.DefineFunc("Decode(dst interface{}, src interface{})").
	Param("EXT", "user provided extension").
	Param("DT", "the dst type to copy into").
	Param("ST", "the src type to copy from").
	ImportPackage("reflect").
	Declare("var typeOf = reflect.TypeOf").
	ImportFunc(decodeAnything).
	Source(`
{{ $decode := expand "DecodeAnything" "EXT" .EXT "DT" .DT "ST" .ST }}
iter := src.({{.ST|name}})
{{ range $extType := .EXT.ExtTypes }} 
	if iter.GetDecoder("{{$extType|name}}") == nil {
		iter.PrepareDecoder(reflect.TypeOf((*{{$extType|name}})(nil)).Elem())
	}
{{ end }}
{{$decode}}(dst.({{.DT|name}}), iter)
`)
