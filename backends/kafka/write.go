package kafka

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/util"
	"github.com/batchcorp/plumber/validate"
)

// Write is the entry point function for performing write operations in Kafka.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func (k *Kafka) Write(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan<- *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "unable to verify write options")
	}

	if writeOpts.EncodeOptions.EncodeType == encoding.EncodeType_ENCODE_TYPE_CLOUDEVENT {
		return k.writeCloudEvents(ctx, writeOpts, errorCh, messages...)
	}

	writer, err := NewWriter(k.dialer, k.connArgs, writeOpts.Kafka.Args.Topics...)
	if err != nil {
		return errors.Wrap(err, "unable to create new writer")
	}

	defer writer.Close()

	for _, topic := range writeOpts.Kafka.Args.Topics {
		for _, msg := range messages {
			if err := k.write(ctx, writer, writeOpts.Kafka.Args, topic, []byte(writeOpts.Kafka.Args.Key), []byte(msg.Input)); err != nil {
				util.WriteError(k.log, errorCh, fmt.Errorf("unable to write message to topic '%s': %s", topic, err))
			}
		}
	}

	return nil
}

func (k *Kafka) getSaramaConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_6_0_0 // Need this in order for offset bits to work

	connOpts := k.connOpts.GetKafka()

	if connOpts.UseTls {
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: connOpts.TlsSkipVerify,
		}
	}

	if connOpts.SaslType != args.SASLType_NONE {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.User = connOpts.SaslUsername
		cfg.Net.SASL.Password = connOpts.SaslPassword

		cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		if connOpts.SaslType == args.SASLType_SCRAM {
			cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		}
	}

	cfg.Producer.Return.Successes = true

	return cfg
}

func (k *Kafka) writeCloudEvents(ctx context.Context, writeOpts *opts.WriteOptions, errorCh chan<- *records.ErrorRecord, messages ...*records.WriteRecord) error {
	client, err := sarama.NewClient(k.connOpts.GetKafka().Address, k.getSaramaConfig())
	if err != nil {
		err = errors.Wrap(err, "unable to initiate kafka connection")
		util.WriteError(k.log, errorCh, err)
		return err
	}

	defer client.Close()

	for _, topic := range writeOpts.Kafka.Args.Topics {
		sender, err := kafka_sarama.NewSenderFromClient(client, topic)
		if err != nil {
			err = errors.Wrap(err, "unable to create new cloudevents sender")
			util.WriteError(k.log, errorCh, err)
			return err
		}

		c, err := cloudevents.NewClient(sender, cloudevents.WithTimeNow(), cloudevents.WithUUIDs())
		if err != nil {
			util.WriteError(k.log, errorCh, errors.Wrap(err, "failed to create cloudevents client"))
			continue
		}

		for i, msg := range messages {
			e, err := util.GenCloudEvent(writeOpts.EncodeOptions.CloudeventSettings, msg)
			if err != nil {
				util.WriteError(k.log, errorCh, errors.Wrap(err, "unable to generate cloudevents event"))
				continue
			}

			result := c.Send(kafka_sarama.WithMessageKey(ctx, sarama.StringEncoder(e.ID())), *e)
			if cloudevents.IsUndelivered(result) {
				util.WriteError(k.log, errorCh, fmt.Errorf("unable to write cloudevents message to topic '%s': %s", topic, result))
			}

			k.log.Debugf("sent: %d, accepted: %t", i, cloudevents.IsACK(result))
		}

		sender.Close(ctx)
	}

	return nil
}

func validateWriteOptions(opts *opts.WriteOptions) error {
	if opts == nil {
		return validate.ErrEmptyWriteOpts
	}

	if opts.Kafka == nil {
		return validate.ErrEmptyBackendGroup
	}

	if opts.Kafka.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if len(opts.Kafka.Args.Topics) == 0 {
		return errors.New("at least one topic must be defined")
	}

	return nil
}

// Write writes a message to a kafka topic. It is a wrapper for WriteMessages.
func (k *Kafka) write(ctx context.Context, writer *skafka.Writer, writeArgs *args.KafkaWriteArgs, topic string, key, value []byte) error {
	msg := skafka.Message{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	headers := make([]skafka.Header, 0)

	for headerName, headerValue := range writeArgs.Headers {
		headers = append(headers, skafka.Header{
			Key:   headerName,
			Value: []byte(headerValue),
		})
	}

	if len(headers) != 0 {
		msg.Headers = headers
	}

	if err := writer.WriteMessages(ctx, msg); err != nil {
		return errors.Wrap(err, "unable to publish message(s)")
	}

	k.log.Infof("Successfully wrote message to topic '%s'", topic)

	return nil
}
