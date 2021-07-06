package codegen

import (
	"github.com/v2pro/wombat/generic"
)

func init() {
	encodeAnything.ImportFunc(encodeEnum)
}

var encodeEnum = generic.DefineFunc(
	"EncodeEnum(dst DT, src ST)").
	Param("EXT", "user provided extension").
	Param("DT", "the dst type to copy into").
	Param("ST", "the src type to copy from").
	Source(`
dst.WriteInt32(int32(src))
	`)
