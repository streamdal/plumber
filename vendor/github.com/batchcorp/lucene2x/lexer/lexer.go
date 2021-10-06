package lexer

import (
	"github.com/batchcorp/lucene2x/token"
)

type Lexer struct {
	input        string
	position     int
	nextPosition int
	ch           byte
	previousType token.TokenType
}

func New(input string) *Lexer {
	l := &Lexer{input: input}

	l.readChar()

	return l
}

// ReadAll reads all tokens into a slice
func (l *Lexer) ReadAll() []token.Token {
	tokens := make([]token.Token, 0)

	for {
		t := l.NextToken()

		if t.Type == token.EOF {
			tokens = append(tokens, t)
			break
		}

		tokens = append(tokens, t)
	}

	return tokens
}

// NextToken reads and returns the next token
func (l *Lexer) NextToken() token.Token {
	var tok token.Token

	l.skipWhitespace()

	switch l.ch {
	case '(':
		tok = newToken(token.LPAREN, l.ch)
	case ')':
		tok = newToken(token.RPAREN, l.ch)
	case '[':
		tok = newToken(token.LBRACKET, l.ch)
	case ']':
		tok = newToken(token.RBRACKET, l.ch)
	case ':':
		tok = newToken(token.COLON, l.ch)
	case '<':
		if l.peekChar() == '=' {
			tok.Type = token.LESSEQUAL
			tok.Literal = "<="
			l.readChar()
		} else {
			tok = newToken(token.LESSTHAN, l.ch)
		}
	case '>':
		if l.peekChar() == '=' {
			tok.Type = token.GREATEREQUAL
			tok.Literal = "<="
			l.readChar()
		} else {
			tok = newToken(token.GREATERTHAN, l.ch)
		}
	case '"':
		tok.Type = token.STRING
		tok.Literal = l.readString()
	case 0:
		tok.Literal = ""
		tok.Type = token.EOF
	default:
		tok = l.readBlob()
	}

	l.previousType = tok.Type

	l.readChar()
	return tok
}

// readBlob reads a string. The token type is determined by string delimiters
// TODO: break this up into sub-functions? It's too long
func (l *Lexer) readBlob() token.Token {
	var tok token.Token

	// AND
	if l.ch == 'A' && l.peekCharOffset(1) == 'N' && l.peekCharOffset(2) == 'D' {
		tok.Type = token.AND
		tok.Literal = "AND"
		l.readChar()
		l.readChar()
		return tok
	}

	// AND
	if l.ch == 'N' && l.peekCharOffset(1) == 'O' && l.peekCharOffset(2) == 'T' {
		tok.Type = token.NOT
		tok.Literal = "NOT"
		l.readChar()
		l.readChar()
		return tok
	}

	// OR
	if l.ch == 'O' && l.peekChar() == 'R' {
		tok.Type = token.OR
		tok.Literal = "OR"
		l.readChar()
		return tok
	}

	// Range query
	if l.previousType == token.LBRACKET {
		tok.Type = token.RANGE
		tok.Literal = l.readRange()
		return tok
	}

	// Unquoted, this means a number value
	// Previous bracket check just in case this block gets moved around
	if isDigit(l.ch) && l.previousType != token.LBRACKET {
		tok.Type = token.STRING
		tok.Literal = l.readNumber()
		return tok
	}

	// String
	if l.previousType == token.QUERY {
		tok.Type = token.STRING
		tok.Literal = l.readQuery()
		return tok
	}

	// String query
	tok.Type = token.QUERY
	tok.Literal = l.readQuery()
	return tok
}

// readRange reads a lucene date range inside of brackets
func (l *Lexer) readRange() string {
	pos := l.position
	for {
		l.readChar()
		if l.ch == ']' {

			break
		} else if l.ch == 0 {
			// EOF
			break
		}
	}
	ret := l.input[pos:l.position]

	// So we can tokenize the ending bracket, since we read past it at this point
	l.nextPosition--
	l.position--

	return ret
}

// readChar reads a single character
func (l *Lexer) readChar() {
	l.ch = l.peekChar()
	l.position = l.nextPosition
	l.nextPosition += 1
}

// prevChar reads the previous character. Used for escape sequences
func (l *Lexer) prevChar() byte {
	if l.position-1 <= 0 {
		return 0
	}

	return l.input[l.position-1]
}

// peekChar looks at what the next character is. Used for tokens which exceed 1 character
func (l *Lexer) peekChar() byte {
	if l.nextPosition >= len(l.input) {
		return 0
	} else {
		return l.input[l.nextPosition]
	}
}

// peekCharOffset looks at a specific offset of the current position. Used for AND token only at the moment
func (l *Lexer) peekCharOffset(offset int) byte {
	if l.position+offset >= len(l.input) {
		return 0
	} else {
		return l.input[l.position+offset]
	}
}

// newToken is a helper method to keep the switch statement in NextToken a little leaner
func newToken(tokenType token.TokenType, ch byte) token.Token {
	return token.Token{
		Type:    tokenType,
		Literal: string(ch),
	}
}

// readQuery reads a JSON path string into a QUERY token type.
// Stops at a colon which is Lucene's column delimiter
func (l *Lexer) readQuery() string {
	pos := l.position
	for {
		l.readChar()
		if l.ch == ':' || l.ch == 0 {
			break
		}
	}
	return l.input[pos:l.position]
}

// readString reads a quoted string
func (l *Lexer) readString() string {
	pos := l.position + 1
	for {
		l.readChar()
		if (l.ch == '"' && l.prevChar() != '\\') || l.ch == 0 {
			break
		}
	}
	return l.input[pos:l.position]
}

// skipWhitespace ignores characters representative of whitespace
func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func (l *Lexer) readNumber() string {
	pos := l.position
	for isDigit(l.ch) {
		l.readChar()
	}
	ret := l.input[pos:l.position]

	// So we can tokenize the ending parenthesis, since we read past it at this point
	l.position--
	l.nextPosition--

	return ret
}

// Determines if the character is representing an int or float
func isDigit(ch byte) bool {
	return ('0' <= ch && ch <= '9') || ch == '.'
}
