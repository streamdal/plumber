package jsonschema

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
)

var (
	ErrMissingMessageDescriptor = errors.New("md cannot be nil")
)

func InferFromProtobuf(id, title, description string, md *desc.MessageDescriptor) (*JSONSchema, error) {
	if md == nil {
		return nil, ErrMissingMessageDescriptor
	}

	return &JSONSchema{
		Schema:      Draft202012,
		ID:          id,
		Type:        "object",
		Title:       title,
		Description: description,
		Default:     json.RawMessage(`{}`),
		Required:    []string{},
		Properties:  walkFields(md.GetFields(), "#/properties"),
	}, nil
}

func walkFields(fds []*desc.FieldDescriptor, path string) map[string]*Property {
	fields := make(map[string]*Property)

	for _, field := range fds {

		tp := path + "/" + field.GetJSONName()

		// Arrays
		if field.GetLabel() == descriptor.FieldDescriptorProto_LABEL_REPEATED {
			// Array of objects. Proto Example:
			// message Subscription {
			//   message Product {
			//     string product_type = 1;
			//   }
			//   repeated Product stripe_products = 9;
			// }
			if field.GetType() == descriptor.FieldDescriptorProto_TYPE_MESSAGE {
				fields[field.GetJSONName()] = &Property{
					ID:                   tp,
					Type:                 "object",
					Title:                fmt.Sprintf("%s schema", field.GetName()),
					Description:          "",
					Default:              json.RawMessage(`{}`),
					Examples:             nil,
					Properties:           walkFields(field.GetMessageType().GetFields(), tp+"/properties"),
					AdditionalProperties: false,
				}
				continue
			}

			// Array of scalar types. Proto Example:
			// message Product {
			//   `repeated string snippets = 3;`
			// }
			t, def := protoToJSONType(field.GetType())
			fields[field.GetJSONName()] = &Property{
				ID:       tp,
				Type:     "array",
				Title:    fmt.Sprintf("%s schema", field.GetName()),
				Default:  json.RawMessage(`[]`),
				Examples: nil,
				Items: &AnyOf{
					ID: tp + "/items",
					Properties: []*Property{
						{
							ID:      tp + "/items/anyOf/0",
							Type:    t,
							Default: def,
						},
					},
				},
				AdditionalProperties: false,
				AdditionalItems:      true,
			}
			continue
		}

		// Object
		if field.GetType() == descriptor.FieldDescriptorProto_TYPE_MESSAGE {
			fields[field.GetJSONName()] = &Property{
				ID:                   tp,
				Type:                 "object",
				Title:                fmt.Sprintf("%s schema", field.GetName()),
				Description:          "",
				Default:              json.RawMessage(`{}`),
				Examples:             nil,
				Properties:           walkFields(field.GetMessageType().GetFields(), tp+"/properties"),
				AdditionalProperties: false,
			}
			continue
		}

		fields[field.GetJSONName()] = parseScalarPB(field, tp)
	}

	return fields
}

func parseScalarPB(field *desc.FieldDescriptor, path string) *Property {
	t, def := protoToJSONType(field.GetType())

	return &Property{
		ID:                   path,
		Type:                 t,
		Title:                field.GetName(),
		Description:          "",
		Default:              def,
		Examples:             nil, // TODO: field.GetDefaultValue() returns an interface{}, figure it out
		Properties:           nil,
		Items:                nil,
		AdditionalProperties: false,
		AdditionalItems:      false,
	}
}

// protoToJSONType() maps protobuf field data types to parquet data types
func protoToJSONType(protoType descriptor.FieldDescriptorProto_Type) (string, json.RawMessage) {
	switch protoType {
	case descriptor.FieldDescriptorProto_TYPE_DOUBLE:
		fallthrough
	case descriptor.FieldDescriptorProto_TYPE_FLOAT:
		fallthrough
	case descriptor.FieldDescriptorProto_TYPE_UINT32:
		fallthrough
	case descriptor.FieldDescriptorProto_TYPE_FIXED32:
		fallthrough
	case descriptor.FieldDescriptorProto_TYPE_SFIXED32:
		fallthrough
	case descriptor.FieldDescriptorProto_TYPE_INT32:
		fallthrough
	case descriptor.FieldDescriptorProto_TYPE_SINT32:
		fallthrough
	case descriptor.FieldDescriptorProto_TYPE_UINT64:
		fallthrough
	case descriptor.FieldDescriptorProto_TYPE_FIXED64:
		fallthrough
	case descriptor.FieldDescriptorProto_TYPE_SINT64:
		fallthrough
	case descriptor.FieldDescriptorProto_TYPE_INT64:
		fallthrough
	case descriptor.FieldDescriptorProto_TYPE_SFIXED64:
		return "number", json.RawMessage(`0`)

	// Boolean
	case descriptor.FieldDescriptorProto_TYPE_BOOL:
		return "boolean", json.RawMessage(`false`)

	// String
	case descriptor.FieldDescriptorProto_TYPE_ENUM:
		fallthrough
	case descriptor.FieldDescriptorProto_TYPE_STRING:
		fallthrough
	case descriptor.FieldDescriptorProto_TYPE_BYTES:
		return "string", json.RawMessage(`""`)
	}

	return "string", json.RawMessage(`""`)
}
