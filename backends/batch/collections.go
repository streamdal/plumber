package batch

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/cli"
)

// CollectionSchema is used to unmarshal the JSON results of a list collections API call
type CollectionSchema struct {
	ID   string `json:"id,omitempty"`
	Name string `json:"name" header:"Schema"`
	Type string `json:"type"`
}

// Collection is used to unmarshal the JSON results of a list collections API call
type Collection struct {
	ID                string `json:"id" header:"ID"`
	Name              string `json:"name" header:"Name"`
	Token             string `json:"token"`
	Paused            bool   `json:"paused" header:"Is Paused?"`
	*CollectionSchema `json:"schema"`
}

// CollectionOutput is used for displaying collections as a table
type CollectionOutput struct {
	Name       string `header:"Name"`
	ID         string `header:"ID"`
	Token      string `header:"Token"`
	Paused     bool   `header:"Is Paused"`
	SchemaName string `header:"Schema Name"`
	SchemaType string `header:"Schema Type"`
}

// SearchResult is used to unmarshal the JSON results of a search API call
type SearchResult struct {
	Total int               `json:"total"`
	Data  []json.RawMessage `json:"data"`
}

const (
	EnterKey = byte(10)
	PageSize = 25
)

// ListCollections lists all of an account's collections
func ListCollections(opts *cli.Options) error {
	b, err := Try(opts)
	if err != nil {
		log.Fatal(err)
	}

	res, _, err := b.Get("/v1/collection", nil)
	if err != nil {
		return errors.New("unable to get list of collections")
	}

	collections := make([]Collection, 0)

	err = json.Unmarshal(res, &collections)
	if err != nil {
		return errors.New("unable to get list of collections")
	}

	if len(collections) == 0 {
		b.log.Info("You have no collections")
		return nil
	}

	output := make([]CollectionOutput, 0)
	for _, c := range collections {
		output = append(output, CollectionOutput{
			ID:         c.ID,
			Name:       c.Name,
			Token:      c.Token,
			Paused:     c.Paused,
			SchemaName: c.CollectionSchema.Name,
			SchemaType: c.CollectionSchema.Type,
		})
	}

	PrintTable(output)

	return nil
}

// SearchCollection queries a collection
func SearchCollection(opts *cli.Options) error {
	b, err := Try(opts)
	if err != nil {
		log.Fatal(err)
	}

	return b.search(PageSize*opts.Batch.Page, PageSize)
}

// search recursively displays pages of (PageSize) results until no more are available
func (b *Batch) search(from, size int) error {
	p := map[string]interface{}{
		"query": b.Opts.Batch.Query,
		"from":  from,
		"size":  size,
	}

	res, _, err := b.Post("/v1/collection/"+b.Opts.Batch.CollectionID+"/search", p)

	results := &SearchResult{}
	if err := json.Unmarshal(res, results); err != nil {
		b.log.Fatalf("Failed to search collection: %s", err)
	}

	// Our JSON output should be human readable
	m, err := json.MarshalIndent(results.Data, "", "  ")
	if err != nil {
		b.log.Fatalf("Could not display search results: %s", err)
	}

	// Display JSON results
	fmt.Println(string(m))

	// Total is the total number of results for the entire query, not the page
	if results.Total > (from + PageSize) {
		nextPageSize := results.Total - (size + PageSize)

		fmt.Printf("--- Press [Enter] for more %d results ---\n", nextPageSize)

		input, _ := bufio.NewReader(os.Stdin).ReadByte()
		if input == EnterKey {
			return b.search(from+PageSize, PageSize)
		}
	}

	return nil
}
