package batch

import (
	"encoding/json"
	"errors"
	"os"
)

// DestinationOutput is used for displaying destinations as a table
type DestinationOutput struct {
	Name     string `json:"name" header:"Name"`
	ID       string `json:"id" header:"Destination ID"`
	Type     string `json:"type" header:"Type""`
	Archived bool   `json:"archived" header:"Is Archived"`
}

var (
	errDestinationsFailed = errors.New("unable to get list of destinations")
	errNoDestinations     = errors.New("you have no destinations")
)

// ListDestinations lists all of an account's replay destinations
func (b *Batch) ListDestinations() error {
	output, err := b.listDestinations()
	if err != nil {
		return err
	}

	printTable(output, os.Stdout)

	return nil
}

func (b *Batch) listDestinations() ([]DestinationOutput, error) {
	res, _, err := b.Get("/v1/destination", nil)
	if err != nil {
		return nil, errDestinationsFailed
	}

	output := make([]DestinationOutput, 0)

	err = json.Unmarshal(res, &output)
	if err != nil {
		return nil, errDestinationsFailed
	}

	if len(output) == 0 {
		b.Log.Info()
		return nil, errNoDestinations
	}

	return output, nil
}
