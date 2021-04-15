package cdc_postgres

import (
	"crypto/tls"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/printer"
	"github.com/jackc/pgx"
	"github.com/sirupsen/logrus"
)

/*
Example output

{
  "lsn": "0/1684C98",
  "xid": 636,
  "timestamp": 1618433619997678000,
  "Changes": [
    {
      "table": "employees",
      "operation": "update",
      "fields": {
        "age": 41,
        "id": 7,
        "name": "tom"
      }
    }
  ]
}

*/

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
