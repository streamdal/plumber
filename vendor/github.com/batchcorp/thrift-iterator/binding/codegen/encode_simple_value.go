package codegen

import (
	"github.com/v2pro/wombat/generic"
	"reflect"
)

func init() {
	encodeAnything.ImportFunc(encodeSimpleValue)
}

var encodeSimpleValue = generic.DefineFunc(
	"EncodeSimpleValue(dst DT, src ST)").
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
dst.Write{{.ST|opFuncName}}(src)
	`)