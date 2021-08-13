package types

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/backends/kafka"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/serializers"

	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
)

const (
	NoSample             = -1
	SampleOffsetInterval = time.Minute
)

type AttachedStream struct {
	MessageCh chan *records.Message
}

type Read struct {
	AttachedClientsMutex *sync.RWMutex
	AttachedClients      map[string]*AttachedStream
	PlumberID            string
	Config               *protos.Read
	ContextCxl           context.Context
	CancelFunc           context.CancelFunc
	Backend              *kafka.KafkaReader // TODO: have to genercize once backend refactor is done
	MsgDesc              *desc.MessageDescriptor
	SampleStart          int64
	SampleStep           int64
	FallbackSampleRate   time.Duration
	Log                  *logrus.Entry
}

// GetSampleRate gets the number of messages received in SampleOffsetInterval in order to calculate how many
func (r *Read) GetSampleRate(ctx context.Context) (offsetStep int64, offsetStart int64, err error) {
	if err := r.Backend.Reader.SetOffset(skafka.LastOffset); err != nil {
		return 0, 0, errors.Wrap(err, "unable to set latest offset")
	}

	r.Log.Debug("starting sample rate calculation")

	msg, err := r.Backend.Reader.ReadMessage(ctx)
	if err != nil {
		if err == context.DeadlineExceeded {
			err = errors.New("timed out waiting for messages, could not get sample rate")
			return NoSample, NoSample, err
		} else {
			err = fmt.Errorf("unable to read kafka message: %s", err)
			return 0, 0, err
		}
	}

	offsetStart = msg.Offset

	r.Log.Debugf("Got first offset: %d", offsetStart)

	time.Sleep(SampleOffsetInterval)

	if err := r.Backend.Reader.SetOffset(skafka.LastOffset); err != nil {
		return 0, 0, errors.Wrap(err, "unable to set latest offset")
	}

	msg, err = r.Backend.Reader.ReadMessage(ctx)
	if err != nil {
		if err == context.Canceled {
			err = errors.New("context cancelled, could not get sample rate")
			return 0, 0, err
		} else {
			err = fmt.Errorf("unable to read kafka message: %s", err)
			return 0, 0, err
		}
	}

	span := float64(msg.Offset - offsetStart)

	sampleOpts := r.Config.SampleOptions

	var rate float64
	switch sampleOpts.SampleInterval {
	case protos.SampleOptions_MINUTE:
		rate = float64(sampleOpts.SampleRate)
	case protos.SampleOptions_SECOND:
		rate = float64(sampleOpts.SampleRate) * 60
	default:
		return 0, 0, fmt.Errorf("unknown sample interval: '%d'", sampleOpts.SampleInterval)
	}

	offsetStep = int64(math.Round(span / rate))

	r.Log.Debugf("Calculated offsetStep (offset %d - offset %d) / rate %d = %d",
		msg.Offset, offsetStart, int(rate), offsetStep)

	return offsetStep, offsetStart, nil
}

// StartRead is a goroutine that is launched when a read is started. It will continue running until plumber exits
// or a read is stopped via the API
func (r *Read) StartRead() {
	defer r.Backend.Reader.Close()
	r.Config.Active = true

	if r.SampleStart > 0 {
		r.Log.Debugf("Starting read at %d with step %d", r.SampleStart, r.SampleStep)
	}

	for {
		select {
		case <-r.ContextCxl.Done():
			r.Log.Info("StartRead stopped")
			return
		default:
			// noop
		}

		var err error

		if r.SampleStart > 0 {
			r.Backend.Reader.SetOffset(r.SampleStart)
		}

		msg, err := r.Backend.Reader.ReadMessage(r.ContextCxl)
		if err != nil {
			if err == context.Canceled {
				return
			} else {
				r.Log.Errorf("unable to read kafka message: %s", err)
				continue
			}
		}

		payload, err := r.generateKafkaPayload(&msg)
		if err != nil {
			r.Log.Errorf("unable to generate kafka payload: %s", err)
		}

		// Send message payload to all attached streams
		r.AttachedClientsMutex.RLock()
		for id, s := range r.AttachedClients {
			r.Log.Debugf("StartRead message to stream '%s'", id)
			s.MessageCh <- payload
		}
		r.AttachedClientsMutex.RUnlock()

		// Sampled read, increment offset offset
		if r.SampleStart > 0 {
			r.SampleStart += r.SampleStep
		}

		if r.FallbackSampleRate > 0 {
			time.Sleep(r.FallbackSampleRate)
			r.Backend.Reader.SetOffset(skafka.LastOffset)
		}
	}

}

// generateKafkaPayload generates a records.Message protobuf struct from a kafka message struct
func (r *Read) generateKafkaPayload(msg *skafka.Message) (*records.Message, error) {
	var err error
	var data []byte

	if do := r.Config.GetDecodeOptions(); do != nil {
		switch do.Type {
		case encoding.Type_PROTOBUF:
			data, err = DecodeProtobuf(r.MsgDesc, msg.Value)
			if err != nil {
				return nil, errors.Wrap(err, "unable to decode protobuf")
			}
		case encoding.Type_AVRO:
			data, err = serializers.AvroDecode(r.Config.DecodeOptions.GetAvro().Schema, msg.Value)
			if err != nil {
				return nil, errors.Wrap(err, "unable to decode AVRO message")
			}
			fallthrough
		case encoding.Type_JSON_SCHEMA:
			// TODO
			fallthrough
		default:
			data = msg.Value
		}
	}

	return &records.Message{
		MessageId:        uuid.NewV4().String(),
		PlumberId:        r.PlumberID,
		UnixTimestampUtc: time.Now().UTC().UnixNano(),
		Message: &records.Message_Kafka{Kafka: &records.Kafka{
			Topic:     msg.Topic,
			Key:       msg.Key,
			Value:     data,
			Blob:      msg.Value,
			Timestamp: msg.Time.UTC().UnixNano(),
			Offset:    msg.Offset,
			Partition: int32(msg.Partition),
			Headers:   convertKafkaHeadersToProto(msg.Headers),
		}},
	}, nil
}

// convertKafkaHeadersToProto converts type of header slice from segmentio's to our protobuf type
func convertKafkaHeadersToProto(original []skafka.Header) []*records.KafkaHeader {
	converted := make([]*records.KafkaHeader, 0)

	for _, o := range original {
		converted = append(converted, &records.KafkaHeader{
			Key:   o.Key,
			Value: string(o.Value),
		})
	}

	return converted
}

// DecodeProtobuf decodes a protobuf message to json
func DecodeProtobuf(md *desc.MessageDescriptor, message []byte) ([]byte, error) {
	// Decode message
	decoded, err := pb.DecodeProtobufToJSON(dynamic.NewMessage(md), message)
	if err != nil {
		return nil, errors.Wrap(err, "unable to decode protobuf message")
	}

	return decoded, nil
}
