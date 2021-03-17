package cli

import "gopkg.in/alecthomas/kingpin.v2"

type BatchOptions struct {
	CollectionID   string
	CollectionName string
	DestinationID  string
	Notes          string
	Query          string
	Page           int
	ReplayID       string
	SchemaID       string
}

func HandleBatchFlags(batchCmd *kingpin.CmdClause, opts *Options) {
	batchCmd.Command("login", "Login to your Batch.sh account")
	batchCmd.Command("logout", "Logout and clear saved credentials")

	// Destinations
	destinationsCmd := batchCmd.Command("destinations", "Replay destinations")
	destinationsCmd.Command("list", "List replay destinations").Default()

	// Schemas
	schemasCmd := batchCmd.Command("schemas", "Collection schemas")
	schemasCmd.Command("list", "List schemas").Default()

	// Replays
	replayCmd := batchCmd.Command("replays", "Replays")
	replayCmd.Command("list", "List replays").Default()

	// Create Replay

	// Collections
	collectionCmd := batchCmd.Command("collections", "Collections")
	collectionCmd.Command("list", "List collections").Default()

	// Create Collection
	createCmd := collectionCmd.Command("new", "Create a new collection")
	createCmd.Flag("name", "Collection Name").
		Required().
		StringVar(&opts.Batch.CollectionName)

	createCmd.Flag("schema-id", "Schema ID. Can be obtained by running `plumber batch schemas`").
		Required().
		StringVar(&opts.Batch.SchemaID)

	createCmd.Flag("notes", "Any notes for this collection").
		StringVar(&opts.Batch.SchemaID)

	// Search Collections
	searchCmd := batchCmd.Command("search", "Search A Collection")
	searchCmd.Flag("query", "Search a specified collection").
		Default("*").
		StringVar(&opts.Batch.Query)

	searchCmd.Flag("collection-id", "Collection ID").
		StringVar(&opts.Batch.CollectionID)

	searchCmd.Flag("page", "Page of search results to display. A page is 25 results").
		IntVar(&opts.Batch.Page)
}
