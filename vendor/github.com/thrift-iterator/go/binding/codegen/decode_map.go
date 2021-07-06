package codegen

import (
	"reflect"
	"github.com/v2pro/wombat/generic"
)

func init() {
	decodeAnything.ImportFunc(decodeMap)
}

var decodeMap = generic.DefineFunc(
	"DecodeMap(dst DT, src ST)").
	Param("EXT", "user provided extension").
	Param("DT", "the dst type to copy into").
	Param("ST", "the src type to copy from").
	ImportFunc(decodeAnything).
	Generators(
	"ptrMapElem", func(typ reflect.Type) reflect.Type {
		return reflect.PtrTo(typ.Elem().Elem())
	}, "ptrMapKey", func(typ reflect.Type) reflect.Type {
		return reflect.PtrTo(typ.Elem().Key())
	}).
	Source(`
{{ $decodeKey := expand "DecodeAnything" "EXT" .EXT "DT" (.DT|ptrMapKey) "ST" .ST }}
{{ $decodeElem := expand "DecodeAnything" "EXT" .EXT "DT" (.DT|ptrMapElem) "ST" .ST }}
if *dst == nil {
	*dst = {{.DT|elem|name}}{}
}
_, _, length := src.ReadMapHeader()
for i := 0; i < length; i++ {
	newKey := new({{.DT|elem|key|name}})
	{{$decodeKey}}(newKey, src)
	newElem := new({{.DT|elem|elem|name}})
	{{$decodeElem}}(newElem, src)
	(*dst)[*newKey] = *newElem
}`)