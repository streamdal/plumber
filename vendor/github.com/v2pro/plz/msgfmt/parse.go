package msgfmt

import (
	"github.com/v2pro/plz/parse"
	"unicode"
	"github.com/v2pro/plz/parse/read"
	"github.com/v2pro/plz/parse/skip"
	"errors"
)

type lexer struct {
	leftCurly     *leftCurlyToken
	literal       *literalToken
	variable      *variableLexer
	formatter     *formatterLexer
	merge         func(left interface{}, right interface{}) interface{}
	parseLiteral  func(src *parse.Source, literal string) interface{}
	parseVariable func(src *parse.Source, id string) interface{}
	parseFunc     func(src *parse.Source, id string, funcName string, funcArgs []string) interface{}
}

func newLexer(initLexer func(l *lexer)) *lexer {
	l := &lexer{
		leftCurly: &leftCurlyToken{},
		literal:   &literalToken{},
		variable:  newVariableLexer(),
		formatter: newFormatterLexer(),
	}
	l.literal.lexer = l
	l.leftCurly.lexer = l
	l.variable.comma.lexer = l
	initLexer(l)
	return l
}

func (lexer *lexer) Parse(src *parse.Source, precedence int) interface{} {
	var left interface{}
	for src.Error() == nil {
		if left == nil {
			left = parse.Parse(src, lexer, precedence)
		} else {
			left = lexer.merge(left, parse.Parse(src, lexer, precedence))
		}
	}
	return left
}

func (lexer *lexer) PrefixToken(src *parse.Source) parse.PrefixToken {
	switch src.Peek()[0] {
	case '{':
		return lexer.leftCurly
	default:
		return lexer.literal
	}
}

func (lexer *lexer) InfixToken(src *parse.Source) (parse.InfixToken, int) {
	return nil, 0
}

type leftCurlyToken struct {
	lexer *lexer
}

func (token *leftCurlyToken) PrefixParse(src *parse.Source) interface{} {
	src.Consume1('{')
	obj := parse.Parse(src, token.lexer.variable, 0)
	if src.Error() != nil {
		return nil
	}
	id, isId := obj.(string)
	if isId {
		obj = token.lexer.parseVariable(src, id)
	}
	rightFormatter := obj.(Formatter)
	src.Consume1('}')
	return rightFormatter
}

type literalToken struct {
	lexer *lexer
}

func (token *literalToken) PrefixParse(src *parse.Source) interface{} {
	return token.lexer.parseLiteral(src, string(read.AnyExcept1(src, nil, '{')))
}

// {VAR,
// {VAR}
type variableLexer struct {
	comma *commaToken
	id    *idToken
}

func newVariableLexer() *variableLexer {
	return &variableLexer{
		comma: &commaToken{},
		id:    &idToken{},
	}
}

func (lexer *variableLexer) PrefixToken(src *parse.Source) parse.PrefixToken {
	skip.UnicodeSpace(src)
	return lexer.id
}

func (lexer *variableLexer) InfixToken(src *parse.Source) (parse.InfixToken, int) {
	skip.UnicodeSpace(src)
	switch src.Peek()[0] {
	case ',':
		return lexer.comma, parse.DefaultPrecedence
	case '}':
		return nil, 0
	default:
		src.ReportError(errors.New("expect , or }, but found " + string([]byte{src.Peek()[0]})))
		return nil, 0
	}
}

type idToken struct {
}

var patternWhiteSpaceAndPatternSyntax = []*unicode.RangeTable{
	unicode.Pattern_White_Space,
	unicode.Pattern_Syntax,
}

func (token *idToken) PrefixParse(src *parse.Source) interface{} {
	runes := read.UnicodeRanges(src, nil, nil, patternWhiteSpaceAndPatternSyntax)
	return string(runes)
}

type commaToken struct {
	lexer *lexer
}

func (token *commaToken) InfixParse(src *parse.Source, left interface{}) interface{} {
	src.Consume1(',')
	funcInvocation := parse.Parse(src, token.lexer.formatter, 0).(funcInvocation)
	return token.lexer.parseFunc(src, left.(string), funcInvocation.name, funcInvocation.args)
}

// {VAR, FORMATTER,
type formatterLexer struct {
	funcName *funcNameToken
}

func newFormatterLexer() *formatterLexer {
	return &formatterLexer{
		funcName: &funcNameToken{},
	}
}

func (lexer *formatterLexer) PrefixToken(src *parse.Source) parse.PrefixToken {
	skip.UnicodeSpace(src)
	buf, _ := src.PeekN(6)
	str := string(buf)
	switch str {
	case "select":
		panic("not implemented")
	case "plural":
		panic("not implemented")
	default:
		return lexer.funcName
	}
}

func (lexer *formatterLexer) InfixToken(src *parse.Source) (parse.InfixToken, int) {
	return nil, 0
}

type funcNameToken struct {
	lexer *lexer
}

type funcInvocation struct {
	name string
	args []string
}

func (token *funcNameToken) PrefixParse(src *parse.Source) interface{} {
	name := string(read.AnyExcept2(src, nil, ',', '}'))
	var args []string
	for {
		skip.UnicodeSpace(src)
		switch src.Peek1() {
		case ',':
			src.Consume1(',')
			args = append(args, string(read.AnyExcept2(src, nil, ',', '}')))
		case '}':
			return funcInvocation{name, args}
		default:
			src.ReportError(errors.New("expect , or }, but found " + string([]byte{src.Peek()[0]})))
			return funcInvocation{name, args}
		}
	}
}

// {VAR, select, args...}
type selectLexer struct {
}

// {VAR, plural, args...}
type pluralLexer struct {
}
