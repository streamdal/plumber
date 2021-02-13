package cli

import "gopkg.in/alecthomas/kingpin.v2"

type BatchOptions struct {
	Query        string
	CollectionID string
	Page         int
}

func HandleBatchFlags(batchCmd *kingpin.CmdClause, opts *Options) {
	batchCmd.Command("login", "Login to your Batch.sh account")
	batchCmd.Command("logout", "Logout and clear saved credentials")
	batchCmd.Command("destinations", "List replay destinations")
	batchCmd.Command("schemas", "List collection schemas")
	batchCmd.Command("replays", "List replays")
	batchCmd.Command("collections", "List collections")

	searchCmd := batchCmd.Command("search", "Search A Collection")

	searchCmd.Flag("query", "Search a specified collection").
		Default("*").
		StringVar(&opts.Batch.Query)

	searchCmd.Flag("collection-id", "Collection ID").
		StringVar(&opts.Batch.CollectionID)

	searchCmd.Flag("page", "Page of search results to display. A page is 25 results").
		IntVar(&opts.Batch.Page)
}
