package generic

import (
	"strings"
	"fmt"
	"bytes"
	"reflect"
)

func parseSignature(input string) (*funcSignature, error) {
	leftBrace := strings.IndexByte(input, '(')
	if leftBrace == -1 {
		return nil, fmt.Errorf("( not found in " + input)
	}
	funcName := strings.TrimSpace(input[:leftBrace])
	rightBrace := strings.IndexByte(input, ')')
	if rightBrace == -1 {
		return nil, fmt.Errorf(") not found in " + input)
	}
	signature := &funcSignature{
		funcName: funcName,
	}
	params := strings.TrimSpace(input[leftBrace+1:rightBrace])
	if len(params) > 0 {
		for _, param := range strings.Split(params, ",") {
			nameAndType := strings.SplitN(strings.TrimSpace(param), " ", 2)
			signature.funcParams = append(signature.funcParams, funcParam{
				paramName: strings.TrimSpace(nameAndType[0]),
				paramType: strings.TrimSpace(nameAndType[1]),
			})
		}
	} else {
		signature.funcParams = []funcParam{}
	}
	returns := strings.TrimFunc(input[rightBrace:], func(r rune) bool {
		switch r {
		case ' ', '\t', '\r', '\n', '(', ')':
			return true
		}
		return false
	})
	if len(returns) > 0 {
		for _, ret := range strings.Split(returns, ",") {
			nameAndType := strings.SplitN(strings.TrimSpace(ret), " ", 2)
			if len(nameAndType) == 1 {
				nameAndType = []string{"", nameAndType[0]}
			}
			signature.funcReturns = append(signature.funcReturns, funcReturn{
				returnName: strings.TrimSpace(nameAndType[0]),
				returnType: strings.TrimSpace(nameAndType[1]),
			})
		}
	} else {
		signature.funcReturns = []funcReturn{}
	}
	return signature, nil
}

type funcSignature struct {
	funcName    string
	funcParams  []funcParam
	funcReturns []funcReturn
}

type funcParam struct {
	paramName string
	paramType string
}

type funcReturn struct {
	returnName string
	returnType string
}

func (signature *funcSignature) expand(out *bytes.Buffer,
	expandedFuncName string, argMap map[string]interface{}) {
	out.WriteString("\nfunc ")
	out.WriteString(expandedFuncName)
	out.WriteByte('(')
	for i, param := range signature.funcParams {
		if i != 0 {
			out.WriteByte(',')
		}
		out.WriteString(param.paramName)
		out.WriteByte(' ')
		typ, isType := argMap[param.paramType].(reflect.Type)
		if isType {
			out.WriteString(genName(typ))
		} else {
			out.WriteString(param.paramType)
		}
	}
	out.WriteByte(')')
	if len(signature.funcReturns) > 0 {
		out.WriteByte('(')
		for i, ret := range signature.funcReturns {
			if i != 0 {
				out.WriteByte(',')
			}
			out.WriteString(ret.returnName)
			out.WriteByte(' ')
			typ, isType := argMap[ret.returnType].(reflect.Type)
			if isType {
				out.WriteString(genName(typ))
			} else {
				out.WriteString(ret.returnType)
			}
		}
		out.WriteByte(')')
	}
	out.WriteByte('{')
}
