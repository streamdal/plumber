package generic

import (
	"text/template"
	"sync"
	"bytes"
	"errors"
	"github.com/v2pro/quokka/docstore/compiler"
	"github.com/v2pro/plz/countlog"
)

var expandLock = &sync.Mutex{}
var templates = map[string]*template.Template{}
var state = struct {
	pkgPath           string
	out               *bytes.Buffer
	importPackages    map[string]bool
	declarations      map[string]bool
	expandedFuncNames map[string]bool
}{}
var DynamicCompilationEnabled = false

func Expand(funcTemplate *FuncTemplate, templateArgs ...interface{}) interface{} {
	expandLock.Lock()
	defer expandLock.Unlock()
	state.out = bytes.NewBuffer(nil)
	state.importPackages = map[string]bool{}
	state.declarations = map[string]bool{}
	state.expandedFuncNames = map[string]bool{}
	expandedFuncName, err := funcTemplate.expand(templateArgs)
	if err != nil {
		countlog.Error("event!expand func template failed",
			"err", err,
			"funcTemplate", funcTemplate.funcName,
			"templateArgs", templateArgs)
		panic(err.Error())
	}
	if inDeclaringByExample {
		funcDeclarations = append(funcDeclarations, funcDeclaration{
			funcTemplate: funcTemplate,
			templateArgs: templateArgs,
		})
	} else {
		expandedFunc := expandedFuncs[expandedFuncName]
		if expandedFunc != nil {
			return expandedFunc
		}
		if !DynamicCompilationEnabled {
			countlog.Error("event!dynamic compilation disabled. "+
				"please add generic.DeclareFunc to init() and re-run codegen",
				"funcTemplate", funcTemplate.funcName,
				"templateArgs", templateArgs,
				"definedInFile", funcTemplate.definedInFile,
				"expandedFuncName", expandedFuncName)
			panic("dynamic compilation disabled")
		}
	}
	prelog := []byte("package main")
	for importPackage := range state.importPackages {
		prelog = append(prelog, "\nimport \""...)
		prelog = append(prelog, importPackage...)
		prelog = append(prelog, '"')
	}
	for declaration := range state.declarations {
		prelog = append(prelog, '\n')
		prelog = append(prelog, declaration...)
	}
	expandedSource := string(append(prelog, state.out.String()...))
	plugin, err := compiler.DynamicCompile(expandedSource)
	if err != nil {
		panic(err.Error())
	}
	symbol, err := plugin.Lookup(expandedFuncName)
	if err != nil {
		countlog.Error("event!lookup symbol failed",
			"err", err,
			"expandedFuncName", expandedFuncName,
			"expandedSource", expandedSource)
		panic(err.Error())
	}
	return symbol
}

func (funcTemplate *FuncTemplate) expand(templateArgs []interface{}) (string, error) {
	for pkg := range funcTemplate.importedPackages {
		state.importPackages[pkg] = true
	}
	for declaration := range funcTemplate.declarations {
		state.declarations[declaration] = true
	}
	argMap, err := funcTemplate.toArgMap(templateArgs)
	if err != nil {
		return "", err
	}
	localOut := bytes.NewBuffer(nil)
	expandedFuncName := expandSymbolName(funcTemplate.funcName, argMap)
	if state.expandedFuncNames[expandedFuncName] {
		return expandedFuncName, nil
	}
	state.expandedFuncNames[expandedFuncName] = true
	parsedTemplate, err := funcTemplate.parse()
	if err != nil {
		return "", err
	}
	funcTemplate.funcSignature.expand(localOut, expandedFuncName, argMap)
	err = parsedTemplate.Execute(localOut, argMap)
	if err != nil {
		return "", err
	}
	localOut.WriteString("\n}")
	state.out.Write(localOut.Bytes())
	return expandedFuncName, nil
}

func (funcTemplate *FuncTemplate) parse() (*template.Template, error) {
	parsedTemplate := templates[funcTemplate.funcName]
	if parsedTemplate == nil {
		var err error
		parsedTemplate, err = template.New(funcTemplate.funcName).
			Funcs(funcTemplate.generators).
			Parse(funcTemplate.templateSource)
		if err != nil {
			return nil, err
		}
		templates[funcTemplate.funcName] = parsedTemplate
	}
	return parsedTemplate, nil
}

type ArgMap map[string]interface{}

func (funcTemplate *FuncTemplate) toArgMap(templateArgs []interface{}) (ArgMap, error) {
	argMap := ArgMap{}
	params := map[string]TemplateParam{}
	for k, v := range funcTemplate.templateParams {
		params[k] = v
	}
	for i := 0; i < len(templateArgs); i += 2 {
		argName := templateArgs[i].(string)
		_, found := funcTemplate.templateParams[argName]
		if found {
			delete(params, argName)
		} else {
			return nil, errors.New("argument " + argName + " not declared as param")
		}
		argVal := templateArgs[i+1]
		argMap[argName] = argVal
	}
	for _, param := range params {
		if param.DefaultValueProvider == nil {
			return nil, errors.New("missing param " + param.Name)
		}
		argMap[param.Name] = param.DefaultValueProvider(argMap)
	}
	return argMap, nil
}
