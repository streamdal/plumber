package codegen

import (
	"github.com/v2pro/wombat/generic"
)

func init() {
	decodeAnything.ImportFunc(decodePointer)
}

var decodePointer = generic.DefineFunc(
	"DecodePointer(dst DT, src ST)").
	Param("EXT", "user provided extension").
	Param("DT", "the dst type to copy into").
	Param("ST", "the src type to copy from").
	ImportFunc(decodeAnything).
	Source(`
{{ $decode := expand "DecodeAnything" "EXT" .EXT "DT" (.DT|elem) "ST" .ST }}
defDst := new({{ .DT|elem|elem|name }})
{{$decode}}(defDst, src)
*dst = defDst
`)