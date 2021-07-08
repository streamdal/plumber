package codegen

import (
	"github.com/v2pro/wombat/generic"
	"reflect"
)

func init() {
	decodeAnything.ImportFunc(decodeSimpleValue)
}

var decodeSimpleValue = generic.DefineFunc(
	"DecodeSimpleValue(dst DT, src ST)").
	Param("EXT", "user provided extension").
	Param("DT", "the dst type to copy into").
	Param("ST", "the src type to copy from").
	Generators(
	"opFuncName", func(typ reflect.Type) string {
		funName := simpleValueMap[typ.Kind()]
		if funName == "" {
			panic(typ.String() + " is not simple value")
		}
		return funName
	}).
	Source(`
*dst = {{.DT|elem|name}}(src.Read{{.DT|elem|opFuncName}}())
	`)