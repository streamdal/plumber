package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

type IWriter interface {
	Write(ctx context.Context, key, value []byte)
}

type Writer struct {
	Id      string
	Writer  *skafka.Writer
	Options *Options
	log     *logrus.Entry
}

func Write(c *cli.Context) error {
	opts, err := parseOptions(c)
	if err != nil {
		return errors.Wrap(err, "unable to parse options")
	}

	dialer := &skafka.Dialer{
		Timeout: opts.ConnectTimeout,
	}

	if opts.UseInsecureTLS {
		dialer.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	// The dialer timeout does not get utilized under some conditions (such as
	// when kafka is configured to NOT auto create topics) - we need a
	// mechanism to bail out early.
	ctxDeadline, _ := context.WithDeadline(context.Background(), time.Now().Add(opts.ConnectTimeout))

	// Attempt to establish connection on startup
	if _, err := dialer.DialLeader(ctxDeadline, "tcp", opts.Host, opts.Topic, 0); err != nil {
		return fmt.Errorf("unable to create initial connection to host '%s': %s",
			opts.Host, err)
	}

	k := &Kafka{
		Options: opts,
		Dialer:  dialer,
	}

	return k.NewWriter("plumber-writer", opts).Write(opts.Key, opts.Value)
}

// GetWriterByTopic returns a new writer per topic
func (k *Kafka) NewWriter(id string, opts *Options) *Writer {
	writerConfig := skafka.WriterConfig{
		Brokers:   []string{opts.Host},
		Topic:     opts.Topic,
		Dialer:    k.Dialer,
		BatchSize: DefaultBatchSize,
	}

	return &Writer{
		Id:      id,
		Options: opts,
		Writer:  skafka.NewWriter(writerConfig),
		log:     logrus.WithField("writerId", id),
	}
}

// Publish a message into Kafka
func (w *Writer) Write(key, value string) error {

	if err := w.Writer.WriteMessages(w.Options.Context, skafka.Message{
		Key:   []byte(key),
		Value: []byte(value),
	}); err != nil {
		return errors.Wrap(err, "unable to publish message(s)")
	}

	return nil
}
