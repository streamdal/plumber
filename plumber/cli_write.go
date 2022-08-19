package plumber

import (
	"context"
	"sync"
	"time"

	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/pkg/errors"
	"github.com/posthog/posthog-go"

	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/validate"
	"github.com/batchcorp/plumber/writer"
)

// HandleWriteCmd handles write mode
func (p *Plumber) HandleWriteCmd() error {
	if err := validate.WriteOptionsForCLI(p.CLIOptions.Write); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	backend, err := backends.New(p.cliConnOpts)
	if err != nil {
		return errors.Wrap(err, "unable to create new backend")
	}

	value, err := writer.GenerateWriteValue(p.CLIOptions.Write, p.cliFDS)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errorCh := make(chan *records.ErrorRecord, 1)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Fire off a goroutine to (potentially) post usage telemetry
	go func() {
		defer wg.Done()

		event := posthog.Capture{
			Event:      "command_write",
			DistinctId: p.PersistentConfig.PlumberID,
			Properties: map[string]interface{}{
				"backend":     backend.Name(),
				"encode_type": "unset",
			},
		}

		if p.CLIOptions.Write.EncodeOptions != nil {
			event.Properties["encode_type"] = p.CLIOptions.Write.EncodeOptions.EncodeType.String()

			if p.CLIOptions.Write.EncodeOptions.EncodeType == encoding.EncodeType_ENCODE_TYPE_JSONPB {
				// Using FD's or dir?
				if p.CLIOptions.Write.EncodeOptions.ProtobufSettings.ProtobufDescriptorSet != "" {
					event.Properties["protobuf_type"] = "fds"
				} else {
					event.Properties["protobuf_type"] = "dir"
				}

				// Set envelope info
				event.Properties["protobuf_envelope"] = p.CLIOptions.Write.EncodeOptions.ProtobufSettings.ProtobufEnvelopeType.String()
			}
		}

		if err := p.Config.Telemetry.Enqueue(event); err != nil {
			p.log.Errorf("unable to track write event: %s", err)
		}
	}()

	go func() {
		if err := backend.Write(ctx, p.CLIOptions.Write, errorCh, value...); err != nil {
			p.log.Errorf("unable to complete write(s): %s", err)
		}

		cancel()
	}()

	var errRecord *records.ErrorRecord

MAIN:
	for {
		select {
		case errRecord = <-errorCh:
			err = backend.DisplayError(errRecord)
			break MAIN
		case <-ctx.Done():
			p.log.Debug("received quit from context - exiting write")
			break MAIN
		}
	}

	if errRecord == nil {
		p.log.Infof("Successfully wrote '%d' message(s)", len(value))
	}

	wg.Wait()

	return nil
}
