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

// Walker provides acccess to information about the state of the AST walker.
type Walker interface {
	// Ancestors returns a copy of a slice containing all the ancestor nodes
	// of the current node. The first node in the slice is the immediate
	// parent of the current node, the node after that its parent, and so on.
	Ancestors() []Node

	// Parent returns the parent node of the current node or nil if this node
	// does not have a parent node.
	Parent() Node
}

//go:generate mockgen -destination mock_visitor_test.go -package ast -self_package go.uber.org/thriftrw/ast go.uber.org/thriftrw/ast Visitor

// Visitor walks an AST. The Visit function is called on each node of the AST.
// If the function returns a non-nil visitor for any node, that visitor is
// called on the chilren of that node.
type Visitor interface {
	Visit(w Walker, n Node) Visitor
}

// VisitorFunc is a Visitor which visits all the nodes of the AST.
type VisitorFunc func(Walker, Node)

// Visit the given node and its descendants.
func (f VisitorFunc) Visit(w Walker, n Node) Visitor {
	f(w, n)
	return f
}

// MultiVisitor merges the given visitors into a single Visitor.
func MultiVisitor(visitors ...Visitor) Visitor {
	return multiVisitor(visitors)
}

type multiVisitor []Visitor

func (vs multiVisitor) Visit(w Walker, n Node) Visitor {
	newVS := make(multiVisitor, 0, len(vs))
	for _, v := range vs {
		if v := v.Visit(w, n); v != nil {
			newVS = append(newVS, v)
		}
	}
	return newVS
}
