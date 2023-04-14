package util

import (
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func GenCloudEvent(cfg *encoding.CloudEventSettings, msg *records.WriteRecord) (*event.Event, error) {
	if cfg == nil {
		return nil, errors.New("cloud event options cannot be nil")
	}

	if msg == nil {
		return nil, errors.New("write record cannot be nil")
	}

	e := cloudevents.NewEvent(cfg.CeSpecVersion)

	// Try to unmarshal entire input to see if it's a valid cloud event in JSON format
	if err := e.UnmarshalJSON([]byte(msg.Input)); err != nil {
		// Input is not entire cloud event, most likely just plain JSON.
		// Set the input as the data field and then set all other values based on flags.
		e.SetData("application/json", []byte(msg.Input))
	}

	if cfg.CeId != "" {
		e.SetID(cfg.CeId)
	}

	if cfg.CeSource != "" {
		e.SetSource(cfg.CeSource)
	}

	if cfg.CeType != "" {
		e.SetType(cfg.CeType)
	}

	if cfg.CeDataSchema != "" {
		e.SetDataSchema(cfg.CeDataSchema)
	}

	if cfg.CeDataContentType != "" {
		e.SetDataContentType(cfg.CeDataContentType)
	}

	return &e, nil
}
