package read

import "github.com/v2pro/plz/parse"

func AnyExcept1(src *parse.Source, space []byte, b1 byte) []byte {
	for src.Error() == nil {
		buf := src.Peek()
		for i := 0; i < len(buf); i++ {
			b := buf[i]
			if b == b1 {
				src.ConsumeN(i)
				return space
			}
			space = append(space, b)
		}
		src.Consume()
	}
	return space
}

func AnyExcept2(src *parse.Source, space []byte, b1 byte, b2 byte) []byte {
	for src.Error() == nil {
		buf := src.Peek()
		for i := 0; i < len(buf); i++ {
			b := buf[i]
			if b == b1 || b == b2 {
				src.ConsumeN(i)
				return space
			}
			space = append(space, b)
		}
		src.Consume()
	}
	return space
}