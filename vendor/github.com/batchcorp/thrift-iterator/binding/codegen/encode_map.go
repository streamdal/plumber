package codegen

import (
	"github.com/v2pro/wombat/generic"
)

func init() {
	encodeAnything.ImportFunc(encodeMap)
}

var encodeMap = generic.DefineFunc(
	"EncodeMap(dst DT, src ST)").
	Param("EXT", "user provided extension").
	Param("DT", "the dst type to copy into").
	Param("ST", "the src type to copy from").
	ImportFunc(encodeAnything).
	Generators(
	"thriftType", dispatchThriftType).
	Source(`
{{ $encodeKey := expand "EncodeAnything" "EXT" .EXT "DT" .DT "ST" (.ST|key) }}
{{ $encodeElem := expand "EncodeAnything" "EXT" .EXT "DT" .DT "ST" (.ST|elem) }}
dst.WriteMapHeader({{.ST|key|thriftType .EXT}}, {{.ST|elem|thriftType .EXT}}, len(src))
for key, elem := range src {
	{{$encodeKey}}(dst, key)
	{{$encodeElem}}(dst, elem)
}`)