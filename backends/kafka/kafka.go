package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	skafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
)

const (
	DefaultMaxBytes  = 1048576 // 1MB
	DefaultMaxWait   = 50 * time.Millisecond
	DefaultBatchSize = 1
)

// Kafka holds all attributes required for performing a write to Kafka. This
// struct should be instantiated via the kafka.Read(..) func.
type Kafka struct {
	Id      string
	Reader  *skafka.Reader
	Writer  *skafka.Writer
	Options *cli.Options
	log     *logrus.Entry
}

func NewReader(opts *cli.Options) (*skafka.Reader, error) {
	dialer := &skafka.Dialer{
		Timeout: opts.Kafka.Timeout,
	}

	if opts.Kafka.InsecureTLS {
		dialer.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	// The dialer timeout does not get utilized under some conditions (such as
	// when kafka is configured to NOT auto create topics) - we need a
	// mechanism to bail out early.
	ctxDeadline, _ := context.WithDeadline(context.Background(), time.Now().Add(opts.Kafka.Timeout))

	// Attempt to establish connection on startup
	if _, err := dialer.DialLeader(ctxDeadline, "tcp", opts.Kafka.Address, opts.Kafka.Topic, 0); err != nil {
		return nil, fmt.Errorf("unable to create initial connection to host '%s': %s",
			opts.Kafka.Address, err)
	}

	return skafka.NewReader(skafka.ReaderConfig{
		Brokers:       []string{opts.Kafka.Address},
		GroupID:       opts.Kafka.ReadGroupId,
		Topic:         opts.Kafka.Topic,
		Dialer:        dialer,
		MaxWait:       DefaultMaxWait,
		MaxBytes:      DefaultMaxBytes,
		QueueCapacity: 1,
	}), nil
}

func NewWriter(opts *cli.Options) (*skafka.Writer, error) {
	dialer := &skafka.Dialer{
		Timeout: opts.Kafka.Timeout,
	}

	if opts.Kafka.InsecureTLS {
		dialer.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	// The dialer timeout does not get utilized under some conditions (such as
	// when kafka is configured to NOT auto create topics) - we need a
	// mechanism to bail out early.
	ctxDeadline, _ := context.WithDeadline(context.Background(), time.Now().Add(opts.Kafka.Timeout))

	// Attempt to establish connection on startup
	if _, err := dialer.DialLeader(ctxDeadline, "tcp", opts.Kafka.Address, opts.Kafka.Topic, 0); err != nil {
		return nil, fmt.Errorf("unable to create initial connection to host '%s': %s",
			opts.Kafka.Address, err)
	}

	return skafka.NewWriter(skafka.WriterConfig{
		Brokers:   []string{opts.Kafka.Address},
		Topic:     opts.Kafka.Topic,
		Dialer:    dialer,
		BatchSize: DefaultBatchSize,
	}), nil
}
