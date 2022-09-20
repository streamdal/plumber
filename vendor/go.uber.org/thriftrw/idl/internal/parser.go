// Copyright (c) 2021 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package internal

import "go.uber.org/thriftrw/ast"

func init() {
	yyErrorVerbose = true
}

// NodePositions maps (hashable) nodes to their document positions.
type NodePositions map[ast.Node]ast.Position

// ParseResult holds the result of a successful Parse.
type ParseResult struct {
	Program       *ast.Program
	NodePositions NodePositions
}

// Parse parses the given Thrift document.
func Parse(s []byte) (ParseResult, []ParseError) {
	lex := newLexer(s)
	e := yyParse(lex)
	if e == 0 && !lex.parseFailed {
		return ParseResult{
			Program:       lex.program,
			NodePositions: lex.nodePositions,
		}, nil
	}
	return ParseResult{}, lex.errors
}

//go:generate ragel -Z -G2 -o lex.go lex.rl
//go:generate goimports -w ./lex.go

//go:generate goyacc -l thrift.y
//go:generate goimports -w ./y.go

//go:generate ./generated.sh
