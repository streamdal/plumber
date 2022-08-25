package plumber

import (
	"os"

	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
	"github.com/batchcorp/plumber/validate"
	"github.com/pkg/errors"
	"github.com/posthog/posthog-go"
)

// HandleReadCmd handles CLI read mode
func (p *Plumber) HandleReadCmd() error {
	if err := validate.ReadOptionsForCLI(p.CLIOptions.Read); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	backend, err := backends.New(p.cliConnOpts)
	if err != nil {
		return errors.Wrap(err, "unable to create new backend")
	}

	resultCh := make(chan *records.ReadRecord, 1)
	errorCh := make(chan *records.ErrorRecord, 1)

	// backend.Read() blocks
	go func() {
		if err := backend.Read(p.ServiceShutdownCtx, p.CLIOptions.Read, resultCh, errorCh); err != nil {
			p.log.Errorf("unable to complete read for backend '%s': %s", backend.Name(), err)
			os.Exit(0) // Exit out of plumber, since we can't continue
		}

		p.log.Debug("Read() exited, calling MainShutdownFunc()")
		p.MainShutdownFunc()
	}()

	// Fire off a goroutine to (potentially) post usage telemetry
	go p.sendReadTelemetry(backend.Name())

MAIN:
	for {
		var err error

		select {
		case msg := <-resultCh:
			p.log.Debug("HandleReadCmd: received message on resultCh")

			decoded, decodeErr := reader.Decode(p.CLIOptions.Read, p.cliFDS, msg.Payload)
			if decodeErr != nil {
				printer.Errorf("unable to decode message payload for backend '%s': %s", backend.Name(), decodeErr)

				if !p.CLIOptions.Read.Continuous {
					break MAIN
				}

				continue
			}

			msg.Payload = decoded

			err = backend.DisplayMessage(p.CLIOptions, msg)
		case errorMsg := <-errorCh:
			err = backend.DisplayError(errorMsg)
		case <-p.MainShutdownCtx.Done():
			break MAIN
		}

		if err != nil {
			printer.Errorf("unable to display message with '%s' backend: %s", backend.Name(), err)
		}

		if !p.CLIOptions.Read.Continuous {
			<-p.MainShutdownCtx.Done()
			break MAIN
		}
	}

	return nil
}

func (p *Plumber) sendReadTelemetry(backend string) {
	event := posthog.Capture{
		Event:      "command_read",
		DistinctId: p.PersistentConfig.PlumberID,
		Properties: map[string]interface{}{
			"continuous":  p.CLIOptions.Read.Continuous,
			"backend":     backend,
			"decode_type": "unset",
		},
	}

	event.Properties["pretty"] = p.CLIOptions.Read.XCliOptions.Pretty
	event.Properties["json"] = p.CLIOptions.Read.XCliOptions.Json
	event.Properties["verbose_output"] = p.CLIOptions.Read.XCliOptions.VerboseOutput

	if p.CLIOptions.Read.SampleOptions != nil {
		event.Properties["sample_rate"] = p.CLIOptions.Read.SampleOptions.SampleRate
		event.Properties["sample_interval_seconds"] = p.CLIOptions.Read.SampleOptions.SampleIntervalSeconds
	}

	if p.CLIOptions.Read.DecodeOptions != nil {
		event.Properties["decode_type"] = p.CLIOptions.Read.DecodeOptions.DecodeType.String()

		if p.CLIOptions.Read.DecodeOptions.DecodeType == encoding.DecodeType_DECODE_TYPE_PROTOBUF {
			// Using FD's or dir?
			if p.CLIOptions.Read.DecodeOptions.ProtobufSettings.ProtobufDescriptorSet != "" {
				event.Properties["protobuf_type"] = "fds"
			} else {
				event.Properties["protobuf_type"] = "dir"
			}

			// Set envelope info
			event.Properties["protobuf_envelope"] = p.CLIOptions.Read.DecodeOptions.ProtobufSettings.ProtobufEnvelopeType.String()
		}
	}

	p.Config.Telemetry.Enqueue(event)
}
