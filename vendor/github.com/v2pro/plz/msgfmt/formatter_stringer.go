package msgfmt

import "fmt"

type stringerFormatter int

func (formatter stringerFormatter) Format(space []byte, kv []interface{}) []byte {
	return append(space, kv[formatter].(fmt.Stringer).String()...)
}
