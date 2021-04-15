package cdc_postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/batchcorp/pgoutput"
	"github.com/batchcorp/plumber/backends/cdc-postgres/types"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/printer"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func Read(opts *cli.Options) error {
	if err := validateReadOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	db, err := NewService(opts)
	if err != nil {
		return err
	}

	p := &CDCPostgres{
		Options: opts,
		Service: db,
		Log:     logrus.WithField("pkg", "cdc-postgres/read.go"),
		Printer: printer.New(),
	}

	return p.Read()
}

func (p *CDCPostgres) Read() error {
	defer p.Service.Close()
	set := pgoutput.NewRelationSet(nil)

	var changeRecord *types.ChangeRecord
	changeRecord = &types.ChangeRecord{}
	changeRecord.Changes = make([]*types.Change, 0)

	sub := pgoutput.NewSubscription(p.Service, p.Options.CDCPostgres.SlotName, p.Options.CDCPostgres.PublisherName, 0, false)

	handler := func(m pgoutput.Message, _ uint64) error {

		switch v := m.(type) {
		case pgoutput.Begin:
			changeRecord.Timestamp = v.Timestamp.UTC().UnixNano()
			changeRecord.XID = v.XID
		case pgoutput.Commit:
			changeRecord.LSN = pgx.FormatLSN(v.LSN)

			// Advance LSN so we do not read the same messages on re-connect
			sub.AdvanceLSN(v.LSN + 1)

			output, _ := json.MarshalIndent(changeRecord, "", "  ")
			p.Printer.Print(string(output))

			changeRecord = &types.ChangeRecord{}
			changeRecord.Changes = make([]*types.Change, 0)
		case pgoutput.Relation:
			set.Add(v)
		case pgoutput.Insert:
			changeSet, ok := set.Get(v.RelationID)
			if !ok {
				return errors.New("relation not found")
			}
			values, err := set.Values(v.RelationID, v.Row)
			if err != nil {
				return fmt.Errorf("error parsing values: %s", err)
			}

			change := &types.Change{
				Operation: "insert",
				Table:     changeSet.Name,
				Fields:    make(map[string]interface{}, 0),
			}
			for name, value := range values {
				change.Fields[name] = value.Get()
			}
			changeRecord.Changes = append(changeRecord.Changes, change)
		case pgoutput.Update:
			changeSet, ok := set.Get(v.RelationID)
			if !ok {
				return errors.New("relation not found")
			}
			values, err := set.Values(v.RelationID, v.Row)
			if err != nil {
				return fmt.Errorf("error parsing values: %s", err)
			}

			change := &types.Change{
				Operation: "update",
				Table:     changeSet.Name,
				Fields:    make(map[string]interface{}, 0),
			}
			for name, value := range values {
				change.Fields[name] = value.Get()
			}
			changeRecord.Changes = append(changeRecord.Changes, change)
		case pgoutput.Delete:
			changeSet, ok := set.Get(v.RelationID)
			if !ok {
				err := fmt.Errorf("relation not found for '%s'", changeSet.Name)
				p.Log.Error(err)
				return err
			}
			values, err := set.Values(v.RelationID, v.Row)
			if err != nil {
				values = make(map[string]pgtype.Value, 0)
				p.Log.Debugf("Error parsing value: %s", err)
			}

			change := &types.Change{
				Operation: "delete",
				Table:     changeSet.Name,
				Fields:    make(map[string]interface{}, 0),
			}
			for name, value := range values {
				val := value.Get()

				// Deletes will include the primary key with the rest of the fields being empty. Ignore them
				if val == "" {
					continue
				}

				change.Fields[name] = val
			}
			changeRecord.Changes = append(changeRecord.Changes, change)
		}
		return nil
	}
	return sub.Start(context.Background(), 0, handler)
}

// validateReadOptions ensures the correct CLI options are specified for the read action
func validateReadOptions(opts *cli.Options) error {
	return nil
}
