package streamdal

import (
	"encoding/json"
	"errors"
)

// SchemaOutput is used for displaying schemas as a table
type SchemaOutput struct {
	Name     string `header:"Name" json:"name"`
	ID       string `header:"Schema ID" json:"id"`
	Type     string `header:"Type" json:"type"`
	Archived bool   `header:"Is Archived" json:"archived"`
}

var (
	errSchemaFailed = errors.New("unable to get list of schemas")
	errNoSchemas    = errors.New("you have no schemas")
)

// ListSchemas lists all of an account's schemas
func (b *Streamdal) ListSchemas() error {
	output, err := b.listSchemas()
	if err != nil {
		return err
	}

	b.Printer(output)

	return nil
}

func (b *Streamdal) listSchemas() ([]SchemaOutput, error) {

	res, _, err := b.Get("/v1/schema", nil)
	if err != nil {
		return nil, errSchemaFailed
	}

	output := make([]SchemaOutput, 0)

	err = json.Unmarshal(res, &output)
	if err != nil {
		return nil, errSchemaFailed
	}

	if len(output) == 0 {
		return nil, errNoSchemas
	}

	return output, nil
}
