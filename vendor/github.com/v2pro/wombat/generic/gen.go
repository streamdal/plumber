package generic

import (
	"reflect"
	"fmt"
	"crypto/sha1"
	"encoding/base32"
	"runtime"
)

func genName(typ reflect.Type) string {
	if typ.PkgPath() != "" {
		if typ.PkgPath() == state.pkgPath {
			return typ.Name()
		}
		state.importPackages[typ.PkgPath()] = true
	}
	switch typ.Kind() {
	case reflect.Ptr:
		genName(typ.Elem())
	case reflect.Slice:
		genName(typ.Elem())
	case reflect.Array:
		genName(typ.Elem())
	}
	return typ.String()
}

func genKey(typ reflect.Type) reflect.Type {
	return typ.Key()
}

func genElem(typ reflect.Type) reflect.Type {
	return typ.Elem()
}

func genMethod(methodName string, typ reflect.Type) (*reflect.Method, error) {
	method, found := typ.MethodByName(methodName)
	if found {
		return &method, nil
	} else {
		return nil, fmt.Errorf("method %s not found in %s", methodName, typ.String())
	}
}

func genReturnType(method *reflect.Method) reflect.Type {
	return method.Type.Out(0)
}

func hash(source string) string {
	h := sha1.New()
	h.Write([]byte(source))
	h.Write([]byte(runtime.Version()))
	return "g" + base32.StdEncoding.EncodeToString(h.Sum(nil))
}
