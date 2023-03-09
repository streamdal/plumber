package util

import (
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/cloudevent"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func GenCloudEvent(cfg *cloudevent.CloudEventOptions, msg *records.WriteRecord) (*event.Event, error) {
	if cfg == nil {
		return nil, errors.New("cloud event options cannot be nil")
	}

	if msg == nil {
		return nil, errors.New("write record cannot be nil")
	}

	e := cloudevents.NewEvent(cfg.CeSpecVersion)

	e.SetData("application/json", []byte(msg.Input))

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
