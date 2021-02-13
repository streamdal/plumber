package batch

import (
	"encoding/json"
	"errors"
	"log"

	"github.com/batchcorp/plumber/cli"
)

// ReplayCollection is used to unmarshal the JSON results of a list replays API call
type ReplayCollection struct {
	Name string `db:"collection_name" json:"name"`
}

// ReplayDestination is used to unmarshal the JSON results of a list replays API call
type ReplayDestination struct {
	Name string `db:"destination_name" json:"name"`
}

// Replay is used to unmarshal the JSON results of a list replays API call
type Replay struct {
	ID                 string `header:"Replay ID" json:"id"`
	Name               string `header:"Name" json:"name"`
	Type               string `header:"Type" json:"type"`
	Query              string `header:"Query" json:"query"`
	Paused             bool   `header:"Is Paused" json:"paused"`
	*ReplayDestination `json:"destination"`
	*ReplayCollection  `json:"collection"`
}

// ReplayOutput is used for displaying replays as a table
type ReplayOutput struct {
	Name        string `header:"Name" json:"name"`
	ID          string `header:"Replay ID" json:"id"`
	Type        string `header:"Type" json:"type"`
	Query       string `header:"Query" json:"query"`
	Collection  string `header:"Collection Name"`
	Destination string `header:"Destination Name"`
	Paused      bool   `header:"Is Paused" json:"paused"`
}

// ListReplays lists all of an account's replays
func ListReplays(opts *cli.Options) error {
	b, err := Try(opts)
	if err != nil {
		log.Fatal(err)
	}

	res, _, err := b.Get("/v1/replay", nil)
	if err != nil {
		return errors.New("unable to get list of replays")
	}

	replays := make([]Replay, 0)

	err = json.Unmarshal(res, &replays)
	if err != nil {
		return errors.New("unable to get list of replays")
	}

	if len(replays) == 0 {
		b.log.Info("You have no replays")
		return nil
	}

	output := make([]ReplayOutput, 0)
	for _, r := range replays {
		output = append(output, ReplayOutput{
			ID:          r.ID,
			Name:        r.Name,
			Type:        r.Type,
			Query:       r.Query,
			Collection:  r.ReplayCollection.Name,
			Destination: r.ReplayDestination.Name,
			Paused:      r.Paused,
		})
	}

	PrintTable(output)

	return nil
}
