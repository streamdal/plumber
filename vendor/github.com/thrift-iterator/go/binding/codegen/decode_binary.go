package codegen

import (
	"github.com/v2pro/wombat/generic"
)

func init() {
	decodeAnything.ImportFunc(decodingBinary)
}

var decodingBinary = generic.DefineFunc(
	"DecodeBinary(dst DT, src ST)").
	Param("EXT", "user provided extension").
	Param("DT", "the dst type to copy into").
	Param("ST", "the src type to copy from").
	Source(`
*dst = src.ReadBinary()
	`)