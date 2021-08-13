package server

import (
	"context"
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	skafka "github.com/segmentio/kafka-go"

	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber/serializers"
	"github.com/batchcorp/plumber/writer"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
)

func (p *PlumberServer) Write(ctx context.Context, req *protos.WriteRequest) (*protos.WriteResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	backend, err := p.getBackendWrite(req)
	if err != nil {
		return nil, CustomError(common.Code_INVALID_ARGUMENT, err.Error())
	}

	// We only need/want to do this once, so generate and pass to generateWriteValue
	md, err := p.getMessageDescriptor(req.EncodeOptions)
	if err != nil {
		return nil, err
	}

	defer backend.Writer.Close()

	messages := make([]skafka.Message, 0)

	for _, v := range req.Records {
		km := v.GetKafka()

		blob, err := generateWriteValue(md, req.EncodeOptions, km.Value)
		if err != nil {
			p.Log.Errorf("Could not generate write value: %s", err)
			continue
		}
		messages = append(messages, skafka.Message{
			Topic:   km.Topic,
			Key:     km.Key,
			Value:   blob,
			Headers: convertProtoHeadersToKafka(km.GetHeaders()),
		})
	}

	if err := backend.Writer.WriteMessages(ctx, messages...); err != nil {
		err = errors.Wrap(err, "unable to write messages to kafka")
		p.Log.Error(err)

		return &protos.WriteResponse{
			Status: &common.Status{
				Code:      common.Code_DATA_LOSS,
				Message:   err.Error(),
				RequestId: uuid.NewV4().String(),
			},
		}, nil
	}

	logMsg := fmt.Sprintf("%d record(s) written", len(messages))

	p.Log.Info(logMsg)

	return &protos.WriteResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   logMsg,
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

// generateWriteValue encodes the message value using avro/protobuf/etc
func generateWriteValue(md *desc.MessageDescriptor, encodingOpts *encoding.Options, data []byte) ([]byte, error) {
	var err error

	switch encodingOpts.Type {
	case encoding.Type_PROTOBUF:
		data, err = writer.ConvertJSONPBToProtobuf(data, dynamic.NewMessage(md))
		if err != nil {
			return nil, errors.Wrap(err, "unable to convert JSONPB to protobuf")
		}
	case encoding.Type_AVRO:
		data, err = serializers.AvroDecode(encodingOpts.GetAvro().Schema, data)
		if err != nil {
			return nil, errors.Wrap(err, "unable to decode AVRO message")
		}
		fallthrough
	case encoding.Type_JSON_SCHEMA:
		// TODO
	}

	return data, nil
}

// convertProtoHeadersToKafka converts type of header slice from segmentio's to our protobuf type
func convertProtoHeadersToKafka(original []*records.KafkaHeader) []skafka.Header {
	converted := make([]skafka.Header, 0)

	for _, o := range original {
		converted = append(converted, skafka.Header{
			Key:   o.Key,
			Value: []byte(o.Value),
		})
	}

	return converted
}
