package cdc_postgres

import (
	"crypto/tls"
	"fmt"

	"github.com/batchcorp/pgoutput"
	"github.com/batchcorp/plumber/backends/cdc-postgres/types"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/printer"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
	"github.com/pkg/errors"
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
	Options *options.Options
	Service *pgx.ReplicationConn
	Log     *logrus.Entry
	Printer printer.IPrinter
}

func NewService(opts *options.Options) (*pgx.ReplicationConn, error) {
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

func handleInsert(set *pgoutput.RelationSet, v *pgoutput.Insert, changeRecord *types.ChangeRecord) (*types.ChangeRecord, error) {
	changeSet, ok := set.Get(v.RelationID)
	if !ok {
		return nil, errors.New("relation not found")
	}
	values, err := set.Values(v.RelationID, v.Row)
	if err != nil {
		return nil, fmt.Errorf("error parsing values: %s", err)
	}

	record := &types.ChangeRecord{
		LSN:       changeRecord.LSN,
		XID:       changeRecord.XID,
		Timestamp: changeRecord.Timestamp,
		Table:     changeSet.Name,
		Operation: "insert",
		Fields:    make(map[string]interface{}, 0),
	}

	for name, value := range values {
		record.Fields[name] = value.Get()
	}

	return record, nil
}

func handleUpdate(set *pgoutput.RelationSet, v *pgoutput.Update, changeRecord *types.ChangeRecord) (*types.ChangeRecord, error) {
	changeSet, ok := set.Get(v.RelationID)
	if !ok {
		return nil, errors.New("relation not found")
	}
	values, err := set.Values(v.RelationID, v.Row)
	if err != nil {
		return nil, fmt.Errorf("error parsing values: %s", err)
	}

	record := &types.ChangeRecord{
		LSN:       changeRecord.LSN,
		XID:       changeRecord.XID,
		Timestamp: changeRecord.Timestamp,
		Table:     changeSet.Name,
		Operation: "update",
		Fields:    make(map[string]interface{}, 0),
		OldFields: make(map[string]interface{}, 0),
	}

	for name, value := range values {
		record.Fields[name] = value.Get()
	}

	oldValues, err := set.Values(v.RelationID, v.OldRow)
	if err == nil {
		for name, value := range oldValues {
			record.OldFields[name] = value
		}
	}

	return record, nil
}

func handleDelete(set *pgoutput.RelationSet, v *pgoutput.Delete, changeRecord *types.ChangeRecord) (*types.ChangeRecord, error) {
	changeSet, ok := set.Get(v.RelationID)
	if !ok {
		err := fmt.Errorf("relation not found for '%s'", changeSet.Name)
		return nil, err
	}
	values, err := set.Values(v.RelationID, v.Row)
	if err != nil {
		values = make(map[string]pgtype.Value, 0)
		return nil, err
	}

	record := &types.ChangeRecord{
		LSN:       changeRecord.LSN,
		XID:       changeRecord.XID,
		Timestamp: changeRecord.Timestamp,
		Table:     changeSet.Name,
		Operation: "delete",
		Fields:    make(map[string]interface{}, 0),
	}

	for name, value := range values {
		val := value.Get()

		// Deletes will include the primary key with the rest of the fields being empty. Ignore them
		if val == "" {
			continue
		}

		record.Fields[name] = val
	}

	return record, nil
}
