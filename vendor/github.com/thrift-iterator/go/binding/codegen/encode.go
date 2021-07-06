package codegen

import "github.com/v2pro/wombat/generic"

var Encode = generic.DefineFunc("Encode(dst interface{}, src interface{})").
	Param("EXT", "user provided extension").
	Param("DT", "the dst type to copy into").
	Param("ST", "the src type to copy from").
	ImportFunc(encodeAnything).
	ImportPackage("reflect").
	Declare("var typeOf = reflect.TypeOf").
	Source(`
{{ $decode := expand "EncodeAnything" "EXT" .EXT "DT" .DT "ST" .ST }}
stream := dst.({{.DT|name}})
{{ range $extType := .EXT.ExtTypes }} 
	if stream.GetEncoder("{{$extType|name}}") == nil {
		stream.PrepareEncoder(reflect.TypeOf((*{{$extType|name}})(nil)).Elem())
	}
{{ end }}
{{$decode}}(stream, src.({{.ST|name}}))
`)
