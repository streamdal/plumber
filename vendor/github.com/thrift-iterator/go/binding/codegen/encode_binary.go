package codegen

import (
	"github.com/v2pro/wombat/generic"
)

func init() {
	encodeAnything.ImportFunc(encodeBinary)
}

var encodeBinary = generic.DefineFunc(
	"EncodeBinary(dst DT, src ST)").
	Param("EXT", "user provided extension").
	Param("DT", "the dst type to copy into").
	Param("ST", "the src type to copy from").
	Source(`
dst.WriteBinary(src)
	`)