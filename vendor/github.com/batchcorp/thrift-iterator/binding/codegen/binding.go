package codegen

import (
	"reflect"
	"github.com/batchcorp/thrift-iterator/protocol"
	"strings"
	"strconv"
)

var byteArrayType = reflect.TypeOf(([]byte)(nil))

var simpleValueMap = map[reflect.Kind]string{
	reflect.Int:     "Int",
	reflect.Int8:    "Int8",
	reflect.Int16:   "Int16",
	reflect.Int32:   "Int32",
	reflect.Int64:   "Int64",
	reflect.Uint:    "Uint",
	reflect.Uint8:   "Uint8",
	reflect.Uint16:  "Uint16",
	reflect.Uint32:  "Uint32",
	reflect.Uint64:  "Uint64",
	reflect.Float32: "Float32",
	reflect.Float64: "Float64",
	reflect.String:  "String",
	reflect.Bool:    "Bool",
}

var thriftTypeMap = map[reflect.Kind]protocol.TType{
	reflect.Int:     protocol.TypeI64,
	reflect.Int8:    protocol.TypeI08,
	reflect.Int16:   protocol.TypeI16,
	reflect.Int32:   protocol.TypeI32,
	reflect.Int64:   protocol.TypeI64,
	reflect.Uint:    protocol.TypeI64,
	reflect.Uint8:   protocol.TypeI08,
	reflect.Uint16:  protocol.TypeI16,
	reflect.Uint32:  protocol.TypeI32,
	reflect.Uint64:  protocol.TypeI64,
	reflect.Float32: protocol.TypeDouble,
	reflect.Float64: protocol.TypeDouble,
	reflect.String:  protocol.TypeString,
	reflect.Bool:    protocol.TypeBool,
}

func isEnumType(valType reflect.Type) bool {
	if valType.Kind() != reflect.Int64 {
		return false
	}
	_, hasStringMethod := valType.MethodByName("String")
	return hasStringMethod
}

func calcBindings(valType reflect.Type) interface{} {
	bindings := []interface{}{}
	for i := 0; i < valType.NumField(); i++ {
		field := valType.Field(i)
		fieldId := protocol.FieldId(0)
		thriftTag := field.Tag.Get("thrift")
		if thriftTag != "" {
			parts := strings.Split(thriftTag, ",")
			if len(parts) >= 2 {
				n, err := strconv.Atoi(parts[1])
				if err != nil {
					panic("thrift tag must be integer")
				}
				fieldId = protocol.FieldId(n)
			}
		}
		if fieldId == 0 {
			continue
		}
		bindings = append(bindings, map[string]interface{}{
			"fieldId":   fieldId,
			"fieldName": field.Name,
			"fieldType": reflect.PtrTo(field.Type),
		})
	}
	return bindings
}