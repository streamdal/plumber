package options

import (
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

type CDCMongoOptions struct {
	DSN                 string
	Database            string
	Collection          string
	IncludeFullDocument bool
}

func HandleCDCMongoFlags(readCmd, writeCmd, relayCmd *kingpin.CmdClause, opts *Options) {
	rc := readCmd.Command("cdc-mongo", "Mongo CDC")
	addSharedMongoFlags(rc, opts)

	wc := writeCmd.Command("cdc-mongo", "Mongo CDC")
	addSharedMongoFlags(wc, opts)

	// If PLUMBER_RELAY_TYPE is set, use env vars, otherwise use CLI flags
	relayType := os.Getenv("PLUMBER_RELAY_TYPE")

	var rec *kingpin.CmdClause

	if relayType != "" {
		rec = relayCmd
	} else {
		rec = relayCmd.Command("cdc-mongo", "Mongo CDC")
	}

	addSharedMongoFlags(rec, opts)
}

func addSharedMongoFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("dsn", "Dial string for mongo server (Ex: mongodb://localhost:27017)").
		Default("mongodb://localhost:27017").
		Envar("PLUMBER_RELAY_CDCMONGO_DSN").
		StringVar(&opts.CDCMongo.DSN)

	cmd.Flag("database", "Database Name").
		Envar("PLUMBER_RELAY_CDCMONGO_DATABASE").
		StringVar(&opts.CDCMongo.Database)

	cmd.Flag("collection", "Collection Name").
		Envar("PLUMBER_RELAY_CDCMONGO_COLLECTION").
		StringVar(&opts.CDCMongo.Collection)

	cmd.Flag("include-full-document", "Include full document in update in update changes. Default is to "+
		"return deltas only").
		Envar("PLUMBER_RELAY_CDCMONGO_INCLUDE_FULL_DOC").
		BoolVar(&opts.CDCMongo.IncludeFullDocument)
}
