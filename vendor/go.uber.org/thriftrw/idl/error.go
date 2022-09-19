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

package idl

import (
	"bytes"
	"fmt"

	"go.uber.org/thriftrw/ast"
	"go.uber.org/thriftrw/idl/internal"
)

// ParseError is an error type listing parse errors and the positions
// that caused them.
type ParseError struct{ Errors []Error }

// Error holds an error and the position that caused it.
type Error struct {
	Pos ast.Position
	Err error
}

func newParseError(errors []internal.ParseError) error {
	if len(errors) == 0 {
		return nil
	}
	errs := make([]Error, len(errors))
	for i, err := range errors {
		errs[i] = Error{
			Pos: err.Pos,
			Err: err.Err,
		}
	}
	return &ParseError{Errors: errs}
}

func (pe *ParseError) Error() string {
	var buffer bytes.Buffer
	buffer.WriteString("parse error\n")
	for _, pe := range pe.Errors {
		buffer.WriteString(fmt.Sprintf("  line %s: %s\n", pe.Pos, pe.Err))
	}
	return buffer.String()
}
