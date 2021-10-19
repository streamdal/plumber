package jsonschema

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
)

const (
	Draft202012 = "https://json-schema.org/draft/2020-12/schema"
)

type IInferSchema interface {
	Infer(id, title, description string, input []byte) (*JSONSchema, error)
	Unmarshal(schema []byte) (*JSONSchema, error)
}

type JSONSchema struct {
	Schema      string               `json:"$schema"`
	ID          string               `json:"$id"`
	Type        string               `json:"type"`
	Title       string               `json:"title,omitempty"`
	Description string               `json:"description,omitempty"`
	Default     json.RawMessage      `json:"default"`
	Required    []string             `json:"required,omitempty"`
	Properties  map[string]*Property `json:"properties"`
}

type Property struct {
	ID                   string               `json:"$id"`
	Type                 string               `json:"type"`
	Title                string               `json:"title,omitempty"`
	Description          string               `json:"description,omitempty"`
	Default              json.RawMessage      `json:"default"`
	Examples             []json.RawMessage    `json:"examples"`
	Properties           map[string]*Property `json:"properties,omitempty"`
	Items                *AnyOf               `json:"items,omitempty"`
	AdditionalProperties bool                 `json:"additionalProperties"`
	AdditionalItems      bool                 `json:"additionalItems"`
}

type AnyOf struct {
	ID         string      `json:"$id"`
	Properties []*Property `json:"anyOf,omitempty"`
}

// Infer is the main entrypoint of this package. It accepts a byte slice containing JSON and returns an inferred schema
func Infer(id, title, description string, input []byte) (*JSONSchema, error) {
	if !gjson.ValidBytes(input) {
		return nil, errors.New("invalid JSON syntax")
	}

	return &JSONSchema{
		Schema:      Draft202012,
		ID:          id,
		Type:        "object",
		Title:       title,
		Description: description,
		Default:     json.RawMessage(`{}`),
		Required:    []string{},
		Properties:  walkJSON(gjson.ParseBytes(input), "#/properties"),
	}, nil
}

// Unmarshal will take a JSON schema stored in JSON format and convert it back to a struct
func Unmarshal(schema []byte) (*JSONSchema, error) {
	u := &JSONSchema{}
	err := json.Unmarshal(schema, &u)
	if err != nil {
		return nil, err
	}
	return u, nil
}

func walkJSON(result gjson.Result, path string) map[string]*Property {
	fields := make(map[string]*Property)

	result.ForEach(func(key, value gjson.Result) bool {

		var el *Property

		if isEmpty(value) {
			return true
		}

		if value.IsArray() {
			el = parseArray(key, value, path)
		} else if value.IsObject() {
			el = parseObject(key, value, path)
		} else {
			el = parseScalar(key, value, path, "")
		}
		fields[key.String()] = el

		return true
	})

	return fields
}

// Ignore empty values since we aren't certain what they will contain when the schema evolves
func isEmpty(value gjson.Result) bool {
	return strings.ToLower(value.Raw) == "null"
}

// parseObject recursively walks through a JSON object
func parseObject(key, value gjson.Result, path string) *Property {
	fields := make(map[string]*Property)

	var items []*Property

	// Empty key string means it's an object inside of an array, no need to alter path
	if key.String() != "" {
		path = path + "/" + key.String()
	}

	value.ForEach(func(key, value gjson.Result) bool {
		var el *Property

		if value.IsArray() {
			el = parseArray(key, value, path)
		} else if value.IsObject() {
			el = parseObject(key, value, path)
		} else {
			el = parseScalar(key, value, path, "")
		}

		fields[key.String()] = el
		return true
	})

	p := &Property{
		ID:                   path,
		Type:                 "object",
		Title:                fmt.Sprintf("%s schema", key.String()),
		Description:          "",
		Default:              json.RawMessage(`{}`),
		Examples:             []json.RawMessage{[]byte(value.Raw)},
		Properties:           fields,
		AdditionalProperties: false,
	}

	if len(items) > 0 {
		p.Items = &AnyOf{
			ID:         "",
			Properties: items,
		}
	}

	return p
}

// parseArray handles JSON fields where the value is an array
func parseArray(key, value gjson.Result, path string) *Property {

	path = path + "/" + key.String()

	items := make([]*Property, 0)

	var idx int

	var isTuple bool
	var arrayValueType string

	value.ForEach(func(key, value gjson.Result) bool {
		var el *Property

		if value.IsArray() {
			el = parseArray(key, value, fmt.Sprintf("%s/items/anyOf/%d", path, idx))
		} else if value.IsObject() {
			el = parseObject(key, value, fmt.Sprintf("%s/items/anyOf/%d", path, idx))
		} else {
			el = parseScalar(key, value, fmt.Sprintf("%s/items/anyOf/%d", path, idx), fmt.Sprintf("%d", idx))
		}

		if arrayValueType == "" {
			arrayValueType = el.Type
		}

		if arrayValueType != el.Type {
			isTuple = true
		}

		items = append(items, el)
		idx++

		return true
	})

	if !isTuple && len(items) > 0 {
		items = items[0:1]
	}

	return &Property{
		ID:       path,
		Type:     "array",
		Title:    fmt.Sprintf("%s schema", key.String()),
		Default:  json.RawMessage(`[]`),
		Examples: []json.RawMessage{[]byte(value.Raw)},
		Items: &AnyOf{
			ID:         path + "/items",
			Properties: items,
		},
		AdditionalProperties: false,
		AdditionalItems:      true,
	}
}

// parseScalar handles JSON fields where the value is a scalar
func parseScalar(key, value gjson.Result, path, index string) *Property {
	v := value.Type

	var t = "string"
	var def = json.RawMessage(`""`)

	switch v {
	case gjson.True:
		t = "boolean"
		def = json.RawMessage(`false`)
	case gjson.False:
		t = "boolean"
		def = json.RawMessage(`false`)
	case gjson.Number:
		t = "number"
		def = json.RawMessage(`0`)
	case gjson.Null:
		t = "null"
		def = json.RawMessage(`null`)
	}

	var desc string
	if index != "" {
		// Scalar value inside of an array
		desc = fmt.Sprintf("anyOf schema %s", index)
	} else {
		// Scalar value of an object key
		desc = key.String()
	}

	return &Property{
		ID:      path,
		Type:    t,
		Title:   desc,
		Default: def,
		Examples: []json.RawMessage{
			[]byte(value.Raw),
		},
	}
}
