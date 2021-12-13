package rstreams

import (
	"time"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/printer"
)

// DisplayMessage will parse a Read record and print (pretty) output to STDOUT
func (r *RedisStreams) DisplayMessage(cliOpts *opts.CLIOptions, msg *records.ReadRecord) error {
	if err := validateReadRecord(msg); err != nil {
		return errors.Wrap(err, "unable to validate read record")
	}

	record := msg.GetRedisStreams()

	properties := [][]string{
		{"Stream", record.Stream},
		{"Key", record.Key},
	}

	receivedAt := time.Unix(msg.ReceivedAtUnixTsUtc, 0)

	printer.PrintTable(cliOpts, msg.Num, receivedAt, msg.Payload, properties)

	return nil
}

// DisplayError will parse an Error record and print (pretty) output to STDOUT
func (r *RedisStreams) DisplayError(msg *records.ErrorRecord) error {
	printer.DefaultDisplayError(msg)
	return nil
}

func validateReadRecord(msg *records.ReadRecord) error {
	if msg == nil {
		return errors.New("msg cannot be nil")
	}

	if msg.GetRedisStreams() == nil {
		return errors.New("BUG: record in message is nil")
	}

	return nil
}

func generateHeaders(headers []*records.RabbitHeader) [][]string {
	r := make([][]string, 0)
	for _, h := range headers {
		r = append(r, []string{h.Key, h.Value})
	}
	return r
}
