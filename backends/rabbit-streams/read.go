package rabbit_streams

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/validate"
)

func (r *RabbitStreams) Read(ctx context.Context, readOpts *opts.ReadOptions, resultsChan chan *records.ReadRecord, errorChan chan *records.ErrorRecord) error {
	if err := validateReadOptions(readOpts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	if readOpts.RabbitStreams.Args.DeclareStream {
		if err := r.declareStream(readOpts.RabbitStreams.Args); err != nil {
			return errors.Wrap(err, "unable to declare stream")
		}
	}

	var count int64

	handleMessage := func(consumerContext stream.ConsumerContext, msg *amqp.Message) {
		for _, value := range msg.Data {
			count++

			serializedMsg, err := json.Marshal(msg.Value)
			if err != nil {
				errorChan <- &records.ErrorRecord{
					OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
					Error:               errors.Wrap(err, "unable to serialize msg into JSON").Error(),
				}
				return
			}

			t := time.Now().UTC().Unix()

			var header *records.RabbitStreamsHeader
			if msg.Header != nil {
				header = &records.RabbitStreamsHeader{
					Durable:       msg.Header.Durable,
					Priority:      uint32(msg.Header.Priority),
					Ttl:           int64(msg.Header.TTL.Seconds()),
					FirstAcquirer: msg.Header.FirstAcquirer,
					DeliveryCount: msg.Header.DeliveryCount,
				}
			}

			resultsChan <- &records.ReadRecord{
				MessageId:           uuid.NewV4().String(),
				Num:                 count,
				ReceivedAtUnixTsUtc: t,
				Payload:             value,
				XRaw:                serializedMsg,
				Record: &records.ReadRecord_RabbitStreams{
					RabbitStreams: &records.RabbitStreams{
						DeliveryTag:         string(msg.DeliveryTag),
						Format:              msg.Format,
						Header:              header,
						DeliveryAnnotations: convertAnnotations(msg.DeliveryAnnotations),
						SendSettled:         msg.SendSettled,
						StreamName:          readOpts.RabbitStreams.Args.Stream,
						Timestamp:           t,
						Value:               value,
					},
				},
			}
		}

		if !readOpts.Continuous {
			go func() {
				_ = consumerContext.Consumer.Close()
			}()
		}
	}

	consumer, err := r.client.NewConsumer(readOpts.RabbitStreams.Args.Stream,
		handleMessage,
		stream.NewConsumerOptions().
			SetConsumerName(r.connArgs.ClientName).
			SetOffset(r.getOffsetOption(readOpts)))
	if err != nil {
		return errors.Wrap(err, "unable to start rabbitmq streams consumer")
	}

	r.log.Infof("Waiting for messages on stream '%s'...", readOpts.RabbitStreams.Args.Stream)

	closeCh := consumer.NotifyClose()

	select {
	case closeEvent := <-closeCh:
		// TODO: implement reconnect logic
		r.log.Debugf("Stream closed by remote host: %s", closeEvent.Reason)
	case <-ctx.Done():
		return nil
	}

	return nil
}

func (r *RabbitStreams) declareStream(args *args.RabbitStreamsReadArgs) error {
	if err := validateDeclareStreamArgs(args); err != nil {
		return errors.Wrap(err, "invalid declare stream arguments")
	}

	err := r.client.DeclareStream(args.Stream,
		&stream.StreamOptions{
			MaxLengthBytes: stream.ByteCapacity{}.From(args.DeclareStreamSize),
		},
	)
	if err == stream.StreamAlreadyExists {
		logrus.Debug("Stream already exists, ignoring --declare-stream")
		return nil
	}

	if err != nil {
		return errors.Wrap(err, "unable to declare rabbitmq stream")
	}

	return nil
}

// convertAnnotations converts a map[interface{}]interface{} to a map[string]string
func convertAnnotations(annotations amqp.Annotations) map[string]string {
	result := make(map[string]string)

	for key, value := range annotations {
		var k, v string

		switch key.(type) {
		case string:
			k = key.(string)
		case int:
			k = strconv.Itoa(key.(int))
		case int64:
			k = strconv.FormatInt(key.(int64), 10)
		default:
			continue // unknown type
		}

		switch value.(type) {
		case string:
			v = value.(string)
		case int:
			v = strconv.Itoa(value.(int))
		case int64:
			v = strconv.FormatInt(value.(int64), 10)
		}

		result[k] = v
	}

	return result
}

func (r *RabbitStreams) getOffsetOption(readOpts *opts.ReadOptions) stream.OffsetSpecification {
	offset := readOpts.RabbitStreams.Args.GetOffsetOptions()

	if offset.NextOffset {
		return stream.OffsetSpecification{}.Next()
	} else if offset.LastOffset {
		return stream.OffsetSpecification{}.Last()
	} else if offset.FirstOffset {
		return stream.OffsetSpecification{}.First()
	} else if offset.LastConsumed {
		return stream.OffsetSpecification{}.LastConsumed()
	}

	return stream.OffsetSpecification{}.Offset(offset.SpecificOffset)
}

func validateReadOptions(readOpts *opts.ReadOptions) error {
	if readOpts == nil {
		return validate.ErrMissingReadOptions
	}

	if readOpts.RabbitStreams == nil {
		return validate.ErrEmptyBackendGroup
	}

	if readOpts.RabbitStreams.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if readOpts.RabbitStreams.Args.Stream == "" {
		return ErrEmptyStream
	}

	return nil
}

func validateDeclareStreamArgs(args *args.RabbitStreamsReadArgs) error {
	if !args.DeclareStream {
		return nil
	}

	if args.DeclareStreamSize == "" {
		return ErrEmptyStreamSize
	}

	return nil
}
