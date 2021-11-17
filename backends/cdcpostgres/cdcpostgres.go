package cdcpostgres

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/pgoutput"
	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/backends/cdcpostgres/types"
	plumberTypes "github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/validate"
)

const BackendName = "cdc-postgres"

type CDCPostgres struct {
	// Base connection options / non-backend-specific options
	connOpts *opts.ConnectionOptions

	// Backend-specific args
	connArgs *args.PostgresConn

	client *pgx.ReplicationConn
	log    *logrus.Entry
}

func New(connOpts *opts.ConnectionOptions) (*CDCPostgres, error) {
	if err := validateBaseConnOpts(connOpts); err != nil {
		return nil, errors.Wrap(err, "unable to validate connection options")
	}

	pgOpts := connOpts.GetPostgres()

	config := pgx.ConnConfig{
		Database: pgOpts.Database,
		User:     pgOpts.Username,
		Password: pgOpts.Password,
		Port:     uint16(pgOpts.Port),
	}

	if pgOpts.UseTls {
		config.TLSConfig = &tls.Config{}

		if pgOpts.InsecureTls {
			config.TLSConfig.InsecureSkipVerify = true
		}
	}

	conn, err := pgx.ReplicationConnect(config)
	if err != nil {
		return nil, err
	}

	return &CDCPostgres{
		client:   conn,
		connOpts: connOpts,
		connArgs: pgOpts,
		log:      logrus.WithField("backend", BackendName),
	}, nil

}

func validateBaseConnOpts(opts *opts.ConnectionOptions) error {
	if opts == nil {
		return validate.ErrMissingConnOpts
	}

	if opts.Conn == nil {
		return validate.ErrMissingConnCfg
	}

	if opts.GetPostgres() == nil {
		return validate.ErrMissingConnArgs
	}

	return nil
}

func (c *CDCPostgres) Name() string {
	return BackendName
}

func (c *CDCPostgres) Close(_ context.Context) error {
	return nil
}

func (c *CDCPostgres) Test(_ context.Context) error {
	return plumberTypes.NotImplementedErr
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
