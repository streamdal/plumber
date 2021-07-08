package read

import (
	"unicode"
	"github.com/v2pro/plz/parse"
)

func UnicodeRange(src *parse.Source, space []rune, table *unicode.RangeTable) []rune {
	for {
		r, n := src.PeekRune()
		if unicode.Is(table, r) {
			src.ConsumeN(n)
			space = append(space, r)
			continue
		}
		return space
	}
}

func UnicodeRanges(src *parse.Source, space []rune, includes []*unicode.RangeTable, excludes []*unicode.RangeTable) []rune {
	for {
		r, n := src.PeekRune()
		for _, exclude := range excludes {
			if unicode.Is(exclude, r) {
				return space
			}
		}
		if len(includes) == 0 {
			src.ConsumeN(n)
			space = append(space, r)
			continue
		}
		for _, include := range includes {
			if unicode.Is(include, r) {
				src.ConsumeN(n)
				space = append(space, r)
				continue
			}
		}
		return space
	}
}
