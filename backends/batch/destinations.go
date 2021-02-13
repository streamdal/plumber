package batch

import (
	"encoding/json"
	"errors"
	"log"

	"github.com/batchcorp/plumber/cli"
)

// DestinationOutput is used for displaying destinations as a table
type DestinationOutput struct {
	Name     string `json:"name" header:"Name"`
	ID       string `json:"id" header:"Destination ID"`
	Type     string `json:"type" header:"Type""`
	Archived bool   `json:"archived" header:"Is Archived"`
}

// ListDestinations lists all of an account's replay destinations
func ListDestinations(opts *cli.Options) error {
	b, err := Try(opts)
	if err != nil {
		log.Fatal(err)
	}

	res, _, err := b.Get("/v1/destination", nil)
	if err != nil {
		return errors.New("unable to get list of destinations")
	}

	output := make([]DestinationOutput, 0)

	err = json.Unmarshal(res, &output)
	if err != nil {
		return errors.New("unable to get list of destinations")
	}

	if len(output) == 0 {
		b.log.Info("You have no destinations")
		return nil
	}

	PrintTable(output)

	return nil
}
