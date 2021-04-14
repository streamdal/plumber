package cdc_postgres

import (
	"crypto/tls"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/printer"
	"github.com/jackc/pgx"

	"github.com/sirupsen/logrus"
)

type CDCPostgres struct {
	Options *cli.Options
	Service *pgx.ReplicationConn
	Log     *logrus.Entry
	Printer printer.IPrinter
}

func NewService(opts *cli.Options) (*pgx.ReplicationConn, error) {
	config := pgx.ConnConfig{
		Database: opts.CDCPostgres.DatabaseName,
		User:     opts.CDCPostgres.Username,
		Password: opts.CDCPostgres.Password,
		Port:     opts.CDCPostgres.Port,
	}

	if opts.CDCPostgres.UseTLS {
		config.TLSConfig = &tls.Config{}

		if opts.CDCPostgres.SkipVerifyTLS {
			config.TLSConfig.InsecureSkipVerify = true
		}
	}

	conn, err := pgx.ReplicationConnect(config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
