package codegen

import (
	"github.com/v2pro/wombat/generic"
)

func init() {
	encodeAnything.ImportFunc(encodePointer)
}

var encodePointer = generic.DefineFunc(
	"EncodePointer(dst DT, src ST)").
	Param("EXT", "user provided extension").
	Param("DT", "the dst type to copy into").
	Param("ST", "the src type to copy from").
	ImportFunc(encodeAnything).
	Source(`
{{ $encode := expand "EncodeAnything" "EXT" .EXT "DT" .DT "ST" (.ST|elem) }}
{{$encode}}(dst, *src)
`)