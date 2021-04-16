package cli

import (
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
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
		Envar("PLUMBER_RELAY_RABBIT_USE_TLS").
		BoolVar(&opts.CDCPostgres.UseTLS)

	cmd.Flag("skip-verify-tls", "Skip server cert verification (default: false)").
		Envar("PLUMBER_RELAY_RABBIT_SKIP_VERIFY_TLS").
		BoolVar(&opts.CDCPostgres.SkipVerifyTLS)

	cmd.Flag("host", "Postgres Server Hostname").
		Envar("PLUMBER_RELAY_CDCPOSTGRES_HOSTNAME").
		StringVar(&opts.CDCPostgres.Host)

	cmd.Flag("port", "Postgres Server Port").
		Envar("PLUMBER_RELAY_CDCPOSTGRES_PORT").
		Default("5432").
		Uint16Var(&opts.CDCPostgres.Port)

	cmd.Flag("username", "Postgres Server Username").
		Envar("PLUMBER_RELAY_CDCPOSTGRES_USERNAME").
		StringVar(&opts.CDCPostgres.Username)

	cmd.Flag("password", "Postgres Server Password").
		Envar("PLUMBER_RELAY_CDCPOSTGRES_PASSWORD").
		StringVar(&opts.CDCPostgres.Password)

	cmd.Flag("database", "Postgres Server Database name").
		Envar("PLUMBER_RELAY_CDCPOSTGRES_DATABASE").
		StringVar(&opts.CDCPostgres.DatabaseName)

	cmd.Flag("slot", "CDC Slot name").
		Envar("PLUMBER_RELAY_CDCPOSTGRES_SLOT").
		StringVar(&opts.CDCPostgres.SlotName)

	cmd.Flag("publisher", "CDC Publisher name").
		Envar("PLUMBER_RELAY_CDCPOSTGRES_PUBLISHER").
		StringVar(&opts.CDCPostgres.PublisherName)
}
