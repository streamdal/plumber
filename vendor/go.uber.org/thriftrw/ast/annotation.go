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

import (
	"fmt"
	"strings"
)

// Annotation represents a type annotation. Type annotations are key-value
// pairs in the form,
//
// 	(foo = "bar", baz = "qux")
//
// They may be used to customize the generated code. Annotations are optional
// anywhere in the code where they're accepted and may be skipped completely.
type Annotation struct {
	Name   string
	Value  string
	Line   int
	Column int
}

func (*Annotation) node() {}

func (*Annotation) visitChildren(nodeStack, visitor) {}

func (ann *Annotation) pos() Position { return Position{Line: ann.Line, Column: ann.Column} }

func (ann *Annotation) String() string {
	return fmt.Sprintf("%s = %q", ann.Name, ann.Value)
}

// Annotations returns the annotations for the given node.
func Annotations(n Node) []*Annotation {
	if na, ok := n.(nodeWithAnnotations); ok {
		return na.annotations()
	}
	return nil
}

// FormatAnnotations formats a collection of annotations into a string.
func FormatAnnotations(anns []*Annotation) string {
	if len(anns) == 0 {
		return ""
	}

	as := make([]string, len(anns))
	for i, ann := range anns {
		as[i] = ann.String()
	}

	return "(" + strings.Join(as, ", ") + ")"
}

// Nodes which have annoations can implement this interface.
type nodeWithAnnotations interface {
	Node

	annotations() []*Annotation
}

var (
	_ nodeWithAnnotations = BaseType{}
	_ nodeWithAnnotations = (*Enum)(nil)
	_ nodeWithAnnotations = (*EnumItem)(nil)
	_ nodeWithAnnotations = (*Field)(nil)
	_ nodeWithAnnotations = (*Function)(nil)
	_ nodeWithAnnotations = ListType{}
	_ nodeWithAnnotations = MapType{}
	_ nodeWithAnnotations = (*Service)(nil)
	_ nodeWithAnnotations = SetType{}
	_ nodeWithAnnotations = (*Struct)(nil)
	_ nodeWithAnnotations = (*Typedef)(nil)
)
