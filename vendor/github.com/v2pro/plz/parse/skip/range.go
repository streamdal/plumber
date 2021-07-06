package skip

import (
	"unicode"
	"github.com/v2pro/plz/parse"
)

func UnicodeRange(src *parse.Source, table *unicode.RangeTable) int {
	count := 0
	for {
		r, n := src.PeekRune()
		if unicode.Is(table, r) {
			src.ConsumeN(n)
			count += n
			continue
		}
		return count
	}
}

func UnicodeRanges(src *parse.Source, includes []*unicode.RangeTable, excludes []*unicode.RangeTable) int {
	count := 0
	for {
		r, n := src.PeekRune()
		for _, exclude := range excludes {
			if unicode.Is(exclude, r) {
				return count
			}
		}
		for _, include := range includes {
			if unicode.Is(include, r) {
				src.ConsumeN(n)
				count += n
				continue
			}
		}
		return count
	}
}
