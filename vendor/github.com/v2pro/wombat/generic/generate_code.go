package generic

import (
	"bytes"
	"io/ioutil"
	"fmt"
	"strings"
)

var expandedFuncs = map[string]interface{}{}

func GenerateCode(gopath string, pkgPath string) {
	state.out = bytes.NewBuffer(nil)
	state.importPackages = map[string]bool{
		"github.com/v2pro/wombat/generic": true,
	}
	state.declarations = map[string]bool{}
	state.expandedFuncNames = map[string]bool{}
	state.pkgPath = pkgPath
	pkgName := findPackageName(gopath, pkgPath)
	prelog := []byte(`
package `)
	prelog = append(prelog, pkgName...)
	for _, funcDeclaration := range funcDeclarations {
		funcDeclaration.funcTemplate.expand(funcDeclaration.templateArgs)
	}
	for importPackage := range state.importPackages {
		prelog = append(prelog, '\n')
		prelog = append(prelog, `import "`...)
		prelog = append(prelog, importPackage...)
		prelog = append(prelog, '"')
	}
	prelog = append(prelog, "\nfunc init() {"...)
	for _, funcDeclaration := range funcDeclarations {
		expandedFuncName, err := funcDeclaration.funcTemplate.expand(funcDeclaration.templateArgs)
		if err != nil {
			panic(err.Error())
		}
		for declaration := range funcDeclaration.funcTemplate.declarations {
			state.declarations[declaration] = true
		}
		prelog = append(prelog, '\n')
		prelog = append(prelog, `generic.RegisterExpandedFunc("`...)
		prelog = append(prelog, expandedFuncName...)
		prelog = append(prelog, `",`...)
		prelog = append(prelog, expandedFuncName...)
		prelog = append(prelog, ')')
	}
	prelog = append(prelog, '}')
	for declaration := range state.declarations {
		prelog = append(prelog, '\n')
		prelog = append(prelog, declaration...)
	}
	source := append([]byte(prelog), state.out.Bytes()...)
	err := ioutil.WriteFile(gopath+"/src/"+pkgPath+"/generated.go", source, 0666)
	if err != nil {
		panic(err.Error())
	}
}

func findPackageName(gopath string, pkgPath string) string {
	dir := gopath + "/src/" + pkgPath
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		panic(err.Error())
	}
	for _, file := range files {
		content, err := ioutil.ReadFile(dir + "/" + file.Name())
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			if strings.Index(line, "package ") == 0 {
				return strings.TrimSpace(line[8:])
			}
		}
	}
	panic("can not tell package from .go files in " + dir)
}

func RegisterExpandedFunc(expandedFuncName string, expandedFunc interface{}) {
	expandedFuncs[expandedFuncName] = expandedFunc
}
