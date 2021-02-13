package batch

import (
	"encoding/json"
	"errors"
	"os"
)

// SchemaOutput is used for displaying schemas as a table
type SchemaOutput struct {
	Name     string `header:"Name" json:"name"`
	ID       string `header:"Schema ID" json:"id"`
	Type     string `header:"Type" json:"type"`
	RootType string `header:"Protobuf Root Type" json:"root_type"`
	Archived bool   `header:"Is Archived" json:"archived"`
}

var (
	errSchemaFailed = errors.New("unable to get list of schemas")
	errNoSchemas    = errors.New("you have no schemas")
)

// ListSchemas lists all of an account's schemas
func (b *Batch) ListSchemas() error {
	output, err := b.listSchemas()
	if err != nil {
		return err
	}

	printTable(output, os.Stdout)

	return nil
}

func (b *Batch) listSchemas() ([]SchemaOutput, error) {

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
