package plumber

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/posthog/posthog-go"

	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/streamdal/plumber/backends"
	"github.com/streamdal/plumber/validate"
	"github.com/streamdal/plumber/writer"
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

	// Fire off a goroutine to (potentially) post usage telemetry
	go p.doWriteTelemetry(backend.Name())

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

	return nil
}

func (p *Plumber) doWriteTelemetry(backend string) {
	event := posthog.Capture{
		Event:      "command_write",
		DistinctId: p.PersistentConfig.PlumberID,
		Properties: map[string]interface{}{
			"backend":              backend,
			"encode_type":          "unset",
			"input_as_json_array":  p.CLIOptions.Write.XCliOptions.InputAsJsonArray,
			"input_metadata_items": len(p.CLIOptions.Write.Record.InputMetadata),
		},
	}

	if p.CLIOptions.Write.Record.Input != "" {
		event.Properties["input_type"] = "argument"
	} else if p.CLIOptions.Write.XCliOptions.InputFile != "" {
		event.Properties["input_type"] = "file"
	} else if len(p.CLIOptions.Write.XCliOptions.InputStdin) > 0 {
		event.Properties["input_type"] = "stdin"
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

	p.Config.Telemetry.Enqueue(event)
}
