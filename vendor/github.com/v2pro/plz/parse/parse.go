package parse

import (
	"io"
	"errors"
	"github.com/v2pro/plz/countlog/loglog"
	"unicode/utf8"
	"reflect"
)

func String(input string, lexer Lexer) (interface{}, error) {
	src := NewSourceString(input)
	left := Parse(src, lexer, 0)
	if src.Error() != nil {
		if src.Error() == io.EOF {
			return left, nil
		}
		return nil, src.Error()
	}
	return left, nil
}

func Parse(src *Source, lexer Lexer, precedence int) interface{} {
	token := lexer.PrefixToken(src)
	if token == nil {
		src.ReportError(errors.New("can not parse"))
		return nil
	}
	loglog.Message("prefix", ">>>", reflect.TypeOf(token))
	left := token.PrefixParse(src)
	loglog.Message("prefix", "<<<", reflect.TypeOf(token))
	for {
		if src.Error() != nil {
			return left
		}
		token, infixPrecedence := lexer.InfixToken(src)
		if token == nil {
			return left
		}
		if precedence >= infixPrecedence {
			loglog.Message("precedence skip ", reflect.TypeOf(token), precedence, infixPrecedence)
			return left
		}
		loglog.Message("infix ", ">>>", reflect.TypeOf(token))
		left = token.InfixParse(src, left)
		loglog.Message("infix ", "<<<", reflect.TypeOf(token))
	}
	return left
}

type Source struct {
	err        error
	reader     io.Reader
	current    []byte
	nextList   [][]byte
	buf        []byte
	Attachment interface{}
}

func NewSource(reader io.Reader, buf []byte) (*Source, error) {
	n, err := reader.Read(buf)
	if n == 0 {
		return nil, err
	}
	return &Source{
		reader:  reader,
		current: buf[:n],
		buf:     buf,
	}, nil
}

func NewSourceString(src string) *Source {
	return &Source{
		current: []byte(src),
	}
}

func (src *Source) SetBuffer(buf []byte) {
	src.buf = buf
}

func (src *Source) Peek() []byte {
	return src.current
}

func (src *Source) Peek1() byte {
	return src.current[0]
}

func (src *Source) PeekN(n int) ([]byte, error) {
	if n <= len(src.current) {
		return src.current[:n], nil
	}
	if src.reader == nil {
		return src.current, io.EOF
	}
	peeked := src.current
	for _, next := range src.nextList {
		peeked = append(peeked, next...)
		if len(peeked) >= n {
			return peeked[:n], nil
		}
	}
	for {
		buf := make([]byte, len(src.buf))
		read, err := src.reader.Read(buf)
		next := buf[:read]
		peeked = append(peeked, next...)
		src.nextList = append(src.nextList, next)
		if len(peeked) >= n {
			return peeked[:n], nil
		}
		if err != nil {
			return peeked, err
		}
	}
}

func (src *Source) ConsumeN(n int) {
	for n != 0 && n >= len(src.current) {
		n -= len(src.current)
		src.Consume()
	}
	src.current = src.current[n:]
}

func (src *Source) Consume1(b1 byte) {
	if b1 != src.current[0] {
		src.ReportError(errors.New(
			"expect " + string([]byte{b1}) +
				" but found " + string([]byte{src.current[0]})))
	}
	src.ConsumeN(1)
}

func (src *Source) Consume() {
	if src.reader == nil {
		src.current = nil
		src.ReportError(io.EOF)
		return
	}
	if len(src.nextList) != 0 {
		src.current = src.nextList[0]
		src.nextList = src.nextList[1:]
		return
	}
	n, err := src.reader.Read(src.buf)
	if err != nil {
		src.ReportError(err)
	}
	src.current = src.buf[:n]
}

func (src *Source) PeekRune() (rune, int) {
	n := len(src.current)
	if n < 1 {
		return utf8.RuneError, 1
	}
	p0 := src.current[0]
	x := first[p0]
	if x >= as {
		return utf8.DecodeRune(src.current)
	}
	sz := x & 7
	fullBuf, _ := src.PeekN(int(sz))
	return utf8.DecodeRune(fullBuf)
}

func (src *Source) ReportError(err error) {
	if src.err == nil {
		src.err = err
	}
}

func (src *Source) Error() error {
	return src.err
}

const DefaultPrecedence = 1

type PrefixToken interface {
	PrefixParse(src *Source) interface{}
}

type InfixToken interface {
	InfixParse(src *Source, left interface{}) interface{}
}

type Lexer interface {
	PrefixToken(src *Source) PrefixToken
	InfixToken(src *Source) (InfixToken, int)
}
