package memphis

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/memphisdev/memphis.go"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/validate"
)

func (m *Memphis) Read(ctx context.Context, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord) error {
	if err := validateReadOptions(readOpts); err != nil {
		return errors.Wrap(err, "invalid read options")
	}

	args := readOpts.GetMemphis().Args

	consumer, err := m.client.CreateConsumer(args.Station, args.ConsumerName, memphis.BatchSize(1))
	if err != nil {
		return errors.Wrap(err, "unable to create Memphis consumer")
	}

	defer consumer.StopConsume()

	var count int64

	msgChan := make(chan *memphis.Msg, 0)

	m.log.Info("Listening for message(s) ...")

	consumer.SetContext(ctx)
	handler := getHandler(msgChan, errorChan)
	if err := consumer.Consume(handler); err != nil {
		return errors.Wrap(err, "unable to consume messages")
	}

	// Read forever unless plumber is terminated or --continuous is not specified
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-msgChan:
			count++
			ts := time.Now().UTC().Unix()

			serializedMsg, err := json.Marshal(msg)
			if err != nil {
				errorChan <- &records.ErrorRecord{
					OccurredAtUnixTsUtc: ts,
					Error:               errors.Wrap(err, "unable to serialize message into JSON").Error(),
				}
			}

			resultsChan <- &records.ReadRecord{
				MessageId:           uuid.NewV4().String(),
				Num:                 count,
				Metadata:            msg.GetHeaders(),
				ReceivedAtUnixTsUtc: ts,
				Payload:             msg.Data(),
				XRaw:                serializedMsg,
				Record: &records.ReadRecord_Memphis{
					Memphis: &records.Memphis{
						Value:     msg.Data(),
						Timestamp: ts,
					},
				},
			}

			if !readOpts.Continuous {
				return nil
			}
		}
	}

	return nil
}

func getHandler(msgChan chan *memphis.Msg, errorChan chan *records.ErrorRecord) memphis.ConsumeHandler {
	return func(msgs []*memphis.Msg, err error, ctx context.Context) {
		if err != nil {
			// Ignore pull timeout errors, it will keep retrying
			if strings.Contains(err.Error(), "timeout") {
				return
			}

			errorChan <- &records.ErrorRecord{
				OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
				Error:               errors.Wrap(err, "unable to handle message(s) message").Error(),
			}
		}

		for _, msg := range msgs {
			msgChan <- msg

			if err := msg.Ack(); err != nil {
				errorChan <- &records.ErrorRecord{
					OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
					Error:               errors.Wrap(err, "unable to acknowledge message").Error(),
				}
			}
		}
	}
}

func validateReadOptions(readOpts *opts.ReadOptions) error {
	if readOpts == nil {
		return validate.ErrMissingReadOptions
	}

	if readOpts.Memphis == nil {
		return validate.ErrEmptyBackendGroup
	}

	args := readOpts.Memphis.Args
	if args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if args.Station == "" {
		return ErrEmptyStation
	}

	return nil
}
