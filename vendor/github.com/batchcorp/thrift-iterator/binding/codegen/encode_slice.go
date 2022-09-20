package codegen

import (
	"github.com/v2pro/wombat/generic"
)

func init() {
	encodeAnything.ImportFunc(encodeSlice)
}

var encodeSlice = generic.DefineFunc(
	"EncodeSlice(dst DT, src ST)").
	Param("EXT", "user provided extension").
	Param("DT", "the dst type to copy into").
	Param("ST", "the src type to copy from").
	ImportFunc(encodeAnything).
	Generators(
	"thriftType", dispatchThriftType).
	Source(`
{{ $encodeElem := expand "EncodeAnything" "EXT" .EXT "DT" .DT "ST" (.ST|elem) }}
dst.WriteListHeader({{.ST|elem|thriftType .EXT }}, len(src))
for _, elem := range src {
	{{$encodeElem}}(dst, elem)
}
`)
