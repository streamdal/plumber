package cdc_postgres

import (
	"context"
	"encoding/json"

	"github.com/batchcorp/pgoutput"
	"github.com/batchcorp/plumber/backends/cdc-postgres/types"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/printer"
	"github.com/jackc/pgx"
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

	sub := pgoutput.NewSubscription(p.Service, p.Options.CDCPostgres.SlotName, p.Options.CDCPostgres.PublisherName, 0, false)

	handler := func(m pgoutput.Message, _ uint64) error {
		switch v := m.(type) {
		case pgoutput.Begin:
			changeRecord = &types.ChangeRecord{
				Timestamp: v.Timestamp.UTC().UnixNano(),
				XID:       v.XID,
				LSN:       pgx.FormatLSN(v.LSN),
			}
		case pgoutput.Commit:
			// Advance LSN so we do not read the same messages on re-connect
			sub.AdvanceLSN(v.LSN + 1)
		case pgoutput.Relation:
			set.Add(v)
		case pgoutput.Insert:
			record, err := handleInsert(set, &v, changeRecord)
			if err != nil {
				return err
			}

			p.printRecord(record)
		case pgoutput.Update:
			record, err := handleUpdate(set, &v, changeRecord)
			if err != nil {
				return err
			}

			p.printRecord(record)
		case pgoutput.Delete:
			record, err := handleDelete(set, &v, changeRecord)
			if err != nil {
				return err
			}

			p.printRecord(record)
		}
		return nil
	}
	return sub.Start(context.Background(), 0, handler)
}

func (p *CDCPostgres) printRecord(record *types.ChangeRecord) {
	output, _ := json.MarshalIndent(record, "", "  ")
	p.Printer.Print(string(output))
}

// validateReadOptions ensures the correct CLI options are specified for the read action
func validateReadOptions(opts *cli.Options) error {
	return nil
}
