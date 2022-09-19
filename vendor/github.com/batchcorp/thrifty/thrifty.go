// Package thrifty is used to transform a wire-format thrift message into a JSON representation
// using an IDL definition. This library builds upon github.com/batchcorp/thrift-iterator by utilizing
// the IDL definition in order to properly represent field names and enum values in the output
// instead of IDs.
package thrifty

import (
	"fmt"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"go.uber.org/thriftrw/ast"
	"go.uber.org/thriftrw/idl"

	thrifter "github.com/batchcorp/thrift-iterator"
	"github.com/batchcorp/thrift-iterator/general"
	"github.com/batchcorp/thrift-iterator/protocol"
)

// ParsedIDL is a convenience struct that holds AST representations of Thrift structs and mappings of enum types
type ParsedIDL struct {
	Namespace string
	Structs   map[string]*ast.Struct
	Enums     map[string]map[int32]string
	Typedefs  map[string]struct{}
}

// DecodeWithParsedIDL decodes a thrift message into JSON format using an IDL abstract syntax tree.
// It is recommended to use this method instead of DecodeWithRawIDL if you have multiple messages to
// decode using the same IDL. Before calling this method, you must parse the IDL definition using ParseIDL
func DecodeWithParsedIDL(idlFiles map[string]*ParsedIDL, thriftMsg []byte, structPath string) ([]byte, error) {
	decoded, err := decodeWireFormat(thriftMsg)
	if err != nil {
		return nil, err
	}

	structName, structNamespace, err := parseStructName(structPath)
	if err != nil {
		return nil, err
	}

	namespaceMsgs, ok := idlFiles[structNamespace]
	if !ok {
		return nil, fmt.Errorf("namespace '%s' not found in thrift IDL", structNamespace)
	}

	result, err := structToMap(namespaceMsgs, structName, decoded)
	if err != nil {
		return nil, err
	}

	// jsoniter is needed to marshal map[interface{}]interface{} types
	js, err := jsoniter.Marshal(result)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal decoded thrift message to JSON")
	}

	return js, nil
}

// DecodeWithRawIDL decodes a thrift message with the provided IDL definition and struct name
// It is recommended to use DecodeWithParsedIDL() instead to avoid the overhead of having to parse the IDL
// into an AST on every decode. This method is here for convenience purposes.
func DecodeWithRawIDL(idlDefinition map[string][]byte, thriftMsg []byte, structName string) ([]byte, error) {
	idl, err := ParseIDLFiles(idlDefinition)
	if err != nil {
		return nil, err
	}

	return DecodeWithParsedIDL(idl, thriftMsg, structName)
}

func DecodeWithoutIDL(thriftMsg []byte) ([]byte, error) {
	decoded, err := decodeWireFormat(thriftMsg)
	if err != nil {
		return nil, err
	}

	// jsoniter is needed to marshal map[interface{}]interface{} types
	js, err := jsoniter.Marshal(decoded)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal thrift message to json")
	}
	return js, nil
}

// ParseIDLFiles receives a map of the contents of .thrift IDL files, with the key being the file name
// and returns a map of parsed IDL files with the map key being the namespace.
func ParseIDLFiles(idlFiles map[string][]byte) (map[string]*ParsedIDL, error) {
	ret := make(map[string]*ParsedIDL)

	for path, contents := range idlFiles {
		idl, err := ParseIDL(contents)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to parse IDL file '%s'", path)
		}

		ns, ok := ret[idl.Namespace]
		if !ok {
			// Namespace definition hasn't been seen before, just set
			ret[idl.Namespace] = idl
			continue
		}

		// Merge maps into existing namespace definition
		for k, _ := range idl.Typedefs {
			ns.Typedefs[k] = struct{}{}
		}

		for k, v := range idl.Structs {
			ns.Structs[k] = v
		}

		for k, v := range idl.Enums {
			ns.Enums[k] = v
		}

	}

	return ret, nil
}

// ParseIDL takes an IDL definition and returns a ParsedIDL struct containing AST representations
// of all thrift structs, and a mapping of enum int->string values. All other Thrift IDL
func ParseIDL(data []byte) (*ParsedIDL, error) {
	parsedIDL := &ParsedIDL{
		Namespace: "default",
		Structs:   make(map[string]*ast.Struct),
		Enums:     make(map[string]map[int32]string),
		Typedefs:  make(map[string]struct{}),
	}

	parsed, err := idl.Parse(data)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse IDL")
	}

	// Find the namespace. This will read only the first one as there can be multiple.
	// We might need to require the end-user to specify a "go" scoped namespace in order
	// to choose the namespace reliably. Ex: "namespace go sh.batch.users".
	// Or possibly require a "batch" scoped namespace.
	for _, head := range parsed.Headers {
		ns, ok := head.(*ast.Namespace)
		if !ok {
			continue
		}

		parsedIDL.Namespace = ns.Name
		break
	}

	for _, def := range parsed.Definitions {
		enums, ok := def.(*ast.Enum)
		if ok {
			parsedIDL.Enums[enums.Name] = make(map[int32]string)
			for _, enum := range enums.Items {
				if enum.Value == nil {
					// TODO: enums without values pass IDL parser, I'm guessing they default to iota style?
					continue
				}

				parsedIDL.Enums[enums.Name][int32(*enum.Value)] = enum.Name
			}
		}

		typedef, ok := def.(*ast.Typedef)
		if ok {
			// We just need to know that the typedef exists. It gets treated like any other base type
			parsedIDL.Typedefs[typedef.Name] = struct{}{}
		}

		msg, ok := def.(*ast.Struct)
		if !ok {
			// Ignore non structs
			continue
		}

		parsedIDL.Structs[msg.Name] = msg

		// NOTE: constants are handled by generated code on the producer side,
		// so it's not necessary to handle in this lib
	}

	return parsedIDL, nil
}

func decodeWireFormat(message []byte) (obj *general.Struct, err error) {
	// This is ugly, unfortunately thrift-iterator has panics
	// On benchmarks, this only added <100ns per op
	defer func() {
		if pErr := recover(); pErr != nil {
			err = fmt.Errorf("decodeWireFormat panicked: %s", pErr)
		}
	}()

	if err := thrifter.Unmarshal(message, &obj); err != nil {
		return nil, errors.Wrap(err, "unable to read thrift message")
	}

	return obj, nil
}

func structToMap(idl *ParsedIDL, structName string, decoded *general.Struct) (map[string]interface{}, error) {
	jsonMap := make(map[string]interface{})

	curStruct, err := findStruct(idl, structName)
	if err != nil {
		return nil, err
	}

	for _, field := range curStruct.Fields {
		// Non-base type
		if _, ok := field.Type.(ast.TypeReference); ok {
			// Check if field is an enum
			enums, ok := idl.Enums[field.Type.String()]
			if ok {
				enumID, ok := decoded.Get(protocol.FieldId(field.ID)).(int32)
				if !ok {
					return nil, fmt.Errorf("BUG: could not type assert ID for field '%s' to int32", field.Name)
				}

				jsonMap[field.Name] = enums[enumID]

				continue
			}

			// Check if field is a custom type definition
			if _, ok := idl.Typedefs[field.Type.String()]; ok {
				// Custom type definition. Don't need to do anything, just treat like a normal field
				jsonMap[field.Name] = decoded.Get(protocol.FieldId(field.ID))
				continue
			}

			// Field IDs can be repeated between structs. Recurse down the decoded data
			subType, ok := decoded.Get(protocol.FieldId(field.ID)).(general.Struct)
			if !ok {
				return nil, fmt.Errorf("could not type assert field '%s' to general.Struct", field.Name)
			}

			v, err := structToMap(idl, field.Type.String(), &subType)
			if err != nil {
				return nil, err
			}

			jsonMap[field.Name] = v

			continue

		}

		// Scalar type
		fieldVal := decoded.Get(protocol.FieldId(field.ID))
		jsonMap[field.Name] = fieldVal
	}

	return jsonMap, nil
}

func findStruct(idl *ParsedIDL, structName string) (*ast.Struct, error) {
	// Looking for struct based on an included file. The struct's name is prefixed with the file's name, minus extension
	if strings.Contains(structName, ".") {
		// Included message, split out file name and lookup via just the
		// struct name since we're grouping all structs by namespace
		parts := strings.Split(structName, ".")
		if len(parts) != 2 {
			return nil, fmt.Errorf("unable to handle path '%s'", structName)
		}

		msg, ok := idl.Structs[parts[1]]
		if !ok {
			return nil, fmt.Errorf("unable to find struct '%s' in file '%s'", parts[1], parts[0]+".thrift")
		}

		return msg, nil
	}

	// No file name, just look up using passed structName
	msg, ok := idl.Structs[structName]
	if !ok {
		return nil, fmt.Errorf("unable to find struct '%s' in namespace '%s'", structName, idl.Namespace)
	}

	return msg, nil
}

func parseStructName(in string) (string, string, error) {
	parts := strings.Split(in, ".")
	if len(parts) < 2 {
		return "", "", fmt.Errorf("'%s' must contain a namespace", in)
	}

	return parts[len(parts)-1], strings.Join(parts[0:len(parts)-1], "."), nil
}
