package token

import "strings"

type TokenType string

type Token struct {
	Type    TokenType
	Literal string
}

const (
	EOF = "EOF"

	// Conditionals

	AND = "AND"
	OR  = "OR"
	NOT = "NOT"

	LPAREN   = "("
	RPAREN   = ")"
	LBRACKET = "["
	RBRACKET = "]"

	COLON = ":"

	// STRING is a Query value in string format
	STRING = "String"

	// QUERY is a JSON path query
	QUERY = "Query"

	// RANGE is a Lucene date range.
	// Ex: [2021-03-08T22:29:05Z TO 2021-03-08T22:30:26Z]
	RANGE = "Range"

	// Comparison

	LESSTHAN     = "<"
	GREATERTHAN  = ">"
	LESSEQUAL    = "<="
	GREATEREQUAL = ">="
)

// GetDates returns the from and to dates from a range token
func (t Token) GetDates() (string, string) {
	if t.Type != RANGE {
		return "", ""
	}

	parts := strings.Split(t.Literal, " TO ")
	if len(parts) < 2 {
		return "", ""
	}

	return parts[0], parts[1]
}
