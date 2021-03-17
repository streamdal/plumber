package batch

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/pkg/errors"
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

type DataLake struct {
	ID string `json:"id"`
}

const (
	EnterKey = byte(10)
	PageSize = 25
)

var (
	errNoCollections     = errors.New("you have no collections")
	errCollectionsFailed = errors.New("unable to get list of collections")
	errCreateFailed      = errors.New("failed to create collection")
	errNoDataLake        = errors.New("you have no datalake; please contact batch.sh support")
)

// ListCollections lists all of an account's collections
func (b *Batch) ListCollections() error {
	output, err := b.listCollections()
	if err != nil {
		return err
	}

	b.Printer(output)
	return nil
}

func (b *Batch) listCollections() ([]CollectionOutput, error) {
	res, _, err := b.Get("/v1/collection", nil)
	if err != nil {
		return nil, err
	}

	collections := make([]Collection, 0)

	err = json.Unmarshal(res, &collections)
	if err != nil {
		return nil, errCollectionsFailed
	}

	if len(collections) == 0 {
		return nil, errNoCollections
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

	return output, nil
}

// SearchCollection queries a collection
func (b *Batch) SearchCollection() error {
	return b.search(PageSize*b.Opts.Batch.Page, PageSize, b.Opts.Batch.Page)
}

// search recursively displays pages of (PageSize) results until no more are available
func (b *Batch) search(from, size, page int) error {
	p := map[string]interface{}{
		"query": b.Opts.Batch.Query,
		"from":  from,
		"size":  size,
	}

	res, _, err := b.Post("/v1/collection/"+b.Opts.Batch.CollectionID+"/search", p)

	results := &SearchResult{}
	if err := json.Unmarshal(res, results); err != nil {
		b.Log.Fatalf("Failed to search collection: %s", err)
	}

	// Our JSON output should be human readable
	m, err := json.MarshalIndent(results.Data, "", "  ")
	if err != nil {
		b.Log.Fatalf("Could not display search results: %s", err)
	}

	// Display JSON results
	fmt.Println(string(m))

	// Total is the total number of results for the entire query, not the page
	if results.Total > (from + PageSize) {
		page++

		nextPageSize := PageSize
		remaining := results.Total - (page * PageSize)
		if remaining < PageSize {
			nextPageSize = remaining
		}

		fmt.Printf("--- Press [Enter] for more %d results, %d results total remaining ---\n", nextPageSize, remaining)

		input, _ := bufio.NewReader(os.Stdin).ReadByte()
		if input == EnterKey {
			return b.search(from+PageSize, PageSize, page)
		}
	}

	return nil
}

func (b *Batch) getDataLakeID() (string, error) {
	res, _, err := b.Get("/v1/datalake", nil)
	if err != nil {
		return "", err
	}

	lakes := make([]*DataLake, 0)
	if err := json.Unmarshal(res, &lakes); err != nil {
		return "", errCreateFailed
	}

	if len(lakes) == 0 {
		return "", errNoDataLake
	}

	return lakes[0].ID, nil
}

func (b *Batch) CreateCollection() error {
	// Get datalake ID
	datalakeID, err := b.getDataLakeID()
	if err != nil {
		return err
	}

	// Create collection
	p := map[string]interface{}{
		"schema_id":   b.Opts.Batch.SchemaID,
		"name":        b.Opts.Batch.CollectionName,
		"notes":       b.Opts.Batch.Notes,
		"datalake_id": datalakeID,
	}

	res, code, err := b.Post("/v1/collection", p)
	if err != nil {
		return errCreateFailed
	}

	if code != http.StatusOK {
		errResponse := &BlunderErrorResponse{}
		if err := json.Unmarshal(res, errResponse); err != nil {
			return errCreateFailed
		}

		for _, e := range errResponse.Errors {
			b.Log.Errorf("%s: %s", errCreateFailed, e.Message)
		}

		return nil
	}

	createdCollection := &Collection{}
	if err := json.Unmarshal(res, createdCollection); err != nil {
		return errCreateFailed
	}

	b.Log.Infof("Created collection %s!\n", createdCollection.ID)

	return nil
}
