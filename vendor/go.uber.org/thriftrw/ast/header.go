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

package ast

// HeaderInfo provides a common way to access the line for a header.
type HeaderInfo struct {
	Line int
}

// Header unifies types representing header in the AST.
type Header interface {
	Node

	Info() HeaderInfo
	header()
}

// Include is a request to include another Thrift file.
//
// 	include "shared.thrift"
//
// thriftrw's custom Include-As syntax may be used to change the name under
// which the file is imported.
//
// 	include t "shared.thrift"
type Include struct {
	Path   string
	Name   string
	Line   int
	Column int
}

func (*Include) node()   {}
func (*Include) header() {}

func (i *Include) pos() Position { return Position{Line: i.Line, Column: i.Column} }

func (*Include) visitChildren(nodeStack, visitor) {}

// Info for Include.
func (i *Include) Info() HeaderInfo {
	return HeaderInfo{Line: i.Line}
}

// CppInclude is a request to include a C++-specific header file.
//
//  cpp_include "<unordered_map>"
type CppInclude struct {
	Path   string
	Line   int
	Column int
}

func (*CppInclude) node()   {}
func (*CppInclude) header() {}

func (i *CppInclude) pos() Position { return Position{Line: i.Line, Column: i.Column} }

func (*CppInclude) visitChildren(nodeStack, visitor) {}

// Info for CppInclude.
func (i *CppInclude) Info() HeaderInfo {
	return HeaderInfo{Line: i.Line}
}

// Namespace statements allow users to choose the package name used by the
// generated code in certain languages.
//
// 	namespace py foo.bar
type Namespace struct {
	Scope  string
	Name   string
	Line   int
	Column int
}

func (*Namespace) node()   {}
func (*Namespace) header() {}

func (n *Namespace) pos() Position { return Position{Line: n.Line, Column: n.Column} }

func (*Namespace) visitChildren(nodeStack, visitor) {}

// Info for Namespace.
func (n *Namespace) Info() HeaderInfo {
	return HeaderInfo{Line: n.Line}
}
