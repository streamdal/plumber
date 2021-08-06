package cli

import (
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

type CDCPostgresOptions struct {
	Host          string
	Port          uint16
	Username      string
	Password      string
	DatabaseName  string
	SlotName      string
	PublisherName string
	UseTLS        bool
	SkipVerifyTLS bool
}

func HandleCDCPostgresFlags(readCmd, _, relayCmd *kingpin.CmdClause, opts *Options) {
	rc := readCmd.Command("cdc-postgres", "Postgres Change Data Capture")
	addSharedCDCPostgresFlags(rc, opts)

	// If PLUMBER_RELAY_TYPE is set, use env vars, otherwise use CLI flags
	relayType := os.Getenv("PLUMBER_RELAY_TYPE")

	var rec *kingpin.CmdClause

	if relayType != "" {
		rec = relayCmd
	} else {
		rec = relayCmd.Command("cdc-postgres", "Postgres Change Data Capture")
	}

	addSharedCDCPostgresFlags(rec, opts)
}

func addSharedCDCPostgresFlags(cmd *kingpin.CmdClause, opts *Options) {

	cmd.Flag("use-tls", "Force TLS usage (regardless of DSN) (default: false)").
		Envar("PLUMBER_RELAY_CDCPOSTGRES_USE_TLS").
		BoolVar(&opts.CDCPostgres.UseTLS)

	cmd.Flag("skip-verify-tls", "Skip server cert verification (default: false)").
		Envar("PLUMBER_RELAY_CDCPOSTGRES_SKIP_VERIFY_TLS").
		BoolVar(&opts.CDCPostgres.SkipVerifyTLS)

	cmd.Flag("host", "Postgres server Hostname").
		Envar("PLUMBER_RELAY_CDCPOSTGRES_HOSTNAME").
		Required().
		StringVar(&opts.CDCPostgres.Host)

	cmd.Flag("port", "Postgres server Port").
		Envar("PLUMBER_RELAY_CDCPOSTGRES_PORT").
		Default("5432").
		Uint16Var(&opts.CDCPostgres.Port)

	cmd.Flag("username", "Postgres server Username").
		Envar("PLUMBER_RELAY_CDCPOSTGRES_USERNAME").
		Required().
		StringVar(&opts.CDCPostgres.Username)

	cmd.Flag("password", "Postgres server Password").
		Envar("PLUMBER_RELAY_CDCPOSTGRES_PASSWORD").
		StringVar(&opts.CDCPostgres.Password)

	cmd.Flag("database", "Postgres server Database name").
		Envar("PLUMBER_RELAY_CDCPOSTGRES_DATABASE").
		Required().
		StringVar(&opts.CDCPostgres.DatabaseName)

	cmd.Flag("slot", "CDC Slot name").
		Envar("PLUMBER_RELAY_CDCPOSTGRES_SLOT").
		Required().
		StringVar(&opts.CDCPostgres.SlotName)

	cmd.Flag("publisher", "CDC Publisher name").
		Envar("PLUMBER_RELAY_CDCPOSTGRES_PUBLISHER").
		Required().
		StringVar(&opts.CDCPostgres.PublisherName)
}
