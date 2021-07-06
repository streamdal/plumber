package skip

import (
	"unicode"
	"github.com/v2pro/plz/parse"
)

func Space(src *parse.Source) int {
	count := 0
	for src.Error() == nil {
		buf := src.Peek()
		for i := 0; i < len(buf); i++ {
			b := buf[i]
			switch b {
			case '\t', '\n', '\v', '\f', '\r', ' ':
				count++
				continue
			default:
				src.ConsumeN(i)
				return count
			}
		}
		src.Consume()
	}
	return count
}

func UnicodeSpace(src *parse.Source) int {
	count := 0
	for src.Error() == nil {
		r, n := src.PeekRune()
		if unicode.IsSpace(r) {
			src.ConsumeN(n)
			count += n
			continue
		}
		return count
	}
	return count
}
