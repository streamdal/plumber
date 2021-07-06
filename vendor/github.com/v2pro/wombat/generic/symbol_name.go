package generic

import (
	"reflect"
	"fmt"
	"strings"
	"sort"
	"strconv"
)

type MangledNameProvider interface {
	MangledName() string
}

func expandSymbolName(plainName string, argMap map[string]interface{}) string {
	expanded := []byte(plainName)
	keys := []string{}
	for key := range argMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := argMap[key]
		if value == nil {
			panic("template arg " + key + " is nil")
		}
		expanded = append(expanded, '_')
		expanded = append(expanded, key...)
		switch typedArg := value.(type) {
		case string:
			expanded = append(expanded, '_')
			expanded = append(expanded, typedArg...)
		case bool:
			expanded = append(expanded, '_')
			expanded = strconv.AppendBool(expanded, typedArg)
		case reflect.Type:
			expanded = append(expanded, '_')
			expanded = append(expanded, typeToSymbol(typedArg)...)
		case MangledNameProvider:
			expanded = append(expanded, '_')
			expanded = append(expanded, typedArg.MangledName()...)
		default:
			panic(fmt.Sprintf("unsupported template arg %v of type %s", value, reflect.TypeOf(value).String()))
		}
	}
	return string(expanded)
}

func typeToSymbol(typ reflect.Type) string {
	switch typ.Kind() {
	case reflect.Map:
		return "map_" + typeToSymbol(typ.Key()) + "_to_" + typeToSymbol(typ.Elem())
	case reflect.Slice:
		return "slice_" + typeToSymbol(typ.Elem())
	case reflect.Array:
		return fmt.Sprintf("array_%d_%s", typ.Len(), typeToSymbol(typ.Elem()))
	case reflect.Ptr:
		return "ptr_" + typeToSymbol(typ.Elem())
	default:
		typeName := typ.String()
		typeName = strings.Replace(typeName, ".", "__", -1)
		if strings.Contains(typeName, "{") {
			typeName = hash(typeName)
		}
		return typeName
	}
}
