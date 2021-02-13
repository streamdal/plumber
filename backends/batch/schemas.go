package batch

import (
	"encoding/json"
	"errors"
	"log"

	"github.com/batchcorp/plumber/cli"
)

// SchemaOutput is used for displaying schemas as a table
type SchemaOutput struct {
	Name     string `header:"Name" json:"name"`
	ID       string `header:"Schema ID" json:"id"`
	Type     string `header:"Type" json:"type"`
	RootType string `header:"Protobuf Root Type" json:"root_type"`
	Archived bool   `header:"Is Archived" json:"archived"`
}

// ListSchemas lists all of an account's schemas
func ListSchemas(opts *cli.Options) error {
	b, err := Try(opts)
	if err != nil {
		log.Fatal(err)
	}

	res, _, err := b.Get("/v1/schema", nil)
	if err != nil {
		return errors.New("unable to get list of schemas")
	}

	output := make([]SchemaOutput, 0)

	err = json.Unmarshal(res, &output)
	if err != nil {
		return errors.New("unable to get list of schemas")
	}

	if len(output) == 0 {
		b.log.Info("You have no schemas")
		return nil
	}

	PrintTable(output)

	return nil
}
