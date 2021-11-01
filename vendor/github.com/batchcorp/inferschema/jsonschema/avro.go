package jsonschema

import (
	"encoding/json"
	"strings"

	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
)

type RootAvroSchema struct {
	Type      string          `json:"type"`
	Namespace string          `json:"namespace"`
	Name      string          `json:"name"`
	Fields    json.RawMessage `json:"fields"`
}

type tmpField struct {
	Name     string          `json:"name"`
	Type     json.RawMessage `json:"type"`
	ItemType string          `json:"items,omitempty"`
	Fields   json.RawMessage `json:"fields,omitempty"`
}

func InferFromAvro(id string, avroSchema []byte) (*JSONSchema, error) {

	rs := &RootAvroSchema{}
	if err := json.Unmarshal(avroSchema, rs); err != nil {
		return nil, errors.Wrap(err, "invalid Avro schema")
	}

	fields := gjson.ParseBytes(rs.Fields)

	properties, err := walkJSONAvro(fields, "#/properties")
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse Avro schema")
	}

	return &JSONSchema{
		Schema:      Draft202012,
		ID:          id,
		Type:        "object",
		Title:       rs.Name,
		Description: rs.Namespace,
		Default:     json.RawMessage(`{}`),
		Required:    []string{},
		Properties:  properties,
	}, nil
}

func walkJSONAvro(result gjson.Result, path string) (map[string]*Property, error) {
	// function level var since we'll be encountering errors inside of a closure
	var walkErr error

	fields := make(map[string]*Property)

	result.ForEach(func(key, value gjson.Result) bool {
		// We're using structs here since it's easier than trying to traverse various keys
		// that may or may not be in the order we expect them to be in
		tf := &tmpField{}
		if err := json.Unmarshal([]byte(value.Raw), tf); err != nil {
			walkErr = err
			fields = nil
			return false // break forEach()
		}

		typeField := gjson.ParseBytes(tf.Type)
		if !typeField.IsObject() {
			// Scalar field
			t, def := convertAvroTypeToJSONSChema(string(tf.Type))
			fields[tf.Name] = &Property{
				ID:          path + "/" + tf.Name,
				Type:        t,
				Title:       tf.Name + " schema",
				Description: "",
				Default:     def,
			}
			return true
		}

		// Either a nested object or an array

		subfield := &tmpField{}
		if err := json.Unmarshal(tf.Type, subfield); err != nil {
			walkErr = err
			fields = nil
			return false // break forEach()
		}

		if string(subfield.Type) == `"array"` {
			// Array
			t, def := convertAvroTypeToJSONSChema(subfield.ItemType)
			fields[tf.Name] = &Property{
				ID:          path + "/" + tf.Name,
				Type:        "array",
				Title:       tf.Name + "array schema",
				Description: "",
				Default:     json.RawMessage(`[]`),
				Items: &AnyOf{
					ID: path + "/" + tf.Name + "/items",
					Properties: []*Property{{
						ID:      path + "/" + tf.Name + "/items/anyOf/0",
						Type:    t,
						Default: def,
						Title:   "anyOf schema 0",
					}},
				},
				AdditionalItems: true,
			}
		} else if string(subfield.Type) == `"record"` {
			// Object, keep diving
			properties, err := walkJSONAvro(gjson.ParseBytes(subfield.Fields), path+"/"+tf.Name+"/properties")
			if err != nil {
				fields = nil
				walkErr = err
				return false // break forEach()
			}

			fields[tf.Name] = &Property{
				ID:          path + "/" + tf.Name,
				Type:        "object",
				Title:       "",
				Description: "",
				Default:     nil,
				Examples:    nil,
				Properties:  properties,
			}
		}

		return true
	})

	return fields, walkErr
}

func convertAvroTypeToJSONSChema(v string) (string, json.RawMessage) {
	v = strings.Trim(v, `""`)
	//v = strings.TrimRight(v, `""`)
	var t = "string"
	var def = json.RawMessage(`""`)

	switch v {
	case "boolean":
		t = "boolean"
		def = json.RawMessage(`false`)
	case "long":
		fallthrough
	case "double":
		fallthrough
	case "float":
		fallthrough
	case "int":
		t = "number"
		def = json.RawMessage(`0`)
	}

	return t, def
}
