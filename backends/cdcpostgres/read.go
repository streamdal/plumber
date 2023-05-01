package cdcpostgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/streamdal/pgoutput"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/backends/cdcpostgres/types"
)

func (c *CDCPostgres) Read(ctx context.Context, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord) error {
	if err := validateReadOptions(readOpts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	args := readOpts.Postgres.Args

	// Wrap context so we can cancel on SIGTERM and if --continuous is not specified
	// cancelFunc will only be called on INSERT/UPDATE/DELETE to allow for Begin/Commit/Relation messages to be processed
	cdcCtx, cancelFunc := context.WithCancel(ctx)

	defer c.client.Close()
	set := pgoutput.NewRelationSet(nil)

	var changeRecord *types.ChangeRecord
	var count int64

	sub := pgoutput.NewSubscription(c.client, args.ReplicationSlotName, args.PublisherName, 0, false)

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

			serializedMsg, err := json.Marshal(record)
			if err != nil {
				return errors.Wrap(err, "unable to serialize ChangeRecord msg to JSON")
			}

			count++

			resultsChan <- &records.ReadRecord{
				MessageId:           uuid.NewV4().String(),
				Num:                 count,
				Metadata:            nil,
				ReceivedAtUnixTsUtc: time.Now().UTC().Unix(),
				Payload:             serializedMsg,
				XRaw:                serializedMsg,
				Record: &records.ReadRecord_Postgres{
					Postgres: &records.Postgres{
						Value:     serializedMsg,
						Timestamp: record.Timestamp,
					},
				},
			}

			if !readOpts.Continuous {
				cancelFunc()
			}
		case pgoutput.Update:
			record, err := handleUpdate(set, &v, changeRecord)
			if err != nil {
				return err
			}

			serializedMsg, err := json.Marshal(record)
			if err != nil {
				return errors.Wrap(err, "unable to serialize ChangeRecord msg to JSON")
			}

			count++

			resultsChan <- &records.ReadRecord{
				MessageId:           uuid.NewV4().String(),
				Num:                 count,
				Metadata:            nil,
				ReceivedAtUnixTsUtc: time.Now().UTC().Unix(),
				Payload:             serializedMsg,
				XRaw:                serializedMsg,
				Record: &records.ReadRecord_Postgres{
					Postgres: &records.Postgres{
						Value:     serializedMsg,
						Timestamp: record.Timestamp,
					},
				},
			}

			if !readOpts.Continuous {
				cancelFunc()
			}
		case pgoutput.Delete:
			record, err := handleDelete(set, &v, changeRecord)
			if err != nil {
				return err
			}

			serializedMsg, err := json.Marshal(record)
			if err != nil {
				return errors.Wrap(err, "unable to serialize ChangeRecord msg to JSON")
			}

			count++

			resultsChan <- &records.ReadRecord{
				MessageId:           uuid.NewV4().String(),
				Num:                 count,
				Metadata:            nil,
				ReceivedAtUnixTsUtc: time.Now().UTC().Unix(),
				Payload:             serializedMsg,
				XRaw:                serializedMsg,
				Record: &records.ReadRecord_Postgres{
					Postgres: &records.Postgres{
						Value:     serializedMsg,
						Timestamp: record.Timestamp,
					},
				},
			}

			if !readOpts.Continuous {
				cancelFunc()
			}
		}

		return nil
	}

	c.log.Info("Listening for changes...")

	go func() {
		if err := sub.Start(cdcCtx, 0, handler); err != nil {
			if !errors.Is(err, context.Canceled) {
				c.log.Error(err)
			}
		}
	}()

	<-cdcCtx.Done()
	time.Sleep(time.Second * 1)

	return nil
}

// validateReadOptions ensures the correct CLI options are specified for the read action
func validateReadOptions(readOpts *opts.ReadOptions) error {
	if readOpts.Postgres == nil {
		return errors.New("read options cannot be nil")
	}

	if readOpts.Postgres.Args == nil {
		return errors.New("read option args cannot be nil")
	}

	return nil
}
