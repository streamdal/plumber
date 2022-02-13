package cdcmongo

import (
	"context"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/types"
)

func (m *Mongo) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan<- *records.ErrorRecord, messages ...*records.WriteRecord) error {
	return types.NotImplementedErr
}
