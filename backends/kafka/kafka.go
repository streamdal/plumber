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

const (
	DefaultConnectTimeout = 10 * time.Second
	DefaultGroupId        = "plumber"
	DefaultMaxBytes       = 1048576 // 1MB
	DefaultMaxWait        = 50 * time.Millisecond
	DefaultBatchSize      = 1
)

type IKafka interface {
	NewReader(id, topic string) *Reader
	NewWriter(id, topic string) *Writer
}

type IReader interface {
	Read(ctx context.Context) (skafka.Message, error)
}

type IWriter interface {
	Write(ctx context.Context, key, value []byte)
}

type Kafka struct {
	Options *Options
	Dialer  *skafka.Dialer
}

type Reader struct {
	Id      string
	Reader  *skafka.Reader
	Options *Options
	log     *logrus.Entry
}

type Writer struct {
	Id      string
	Writer  *skafka.Writer
	Options *Options
	log     *logrus.Entry
}

type Options struct {
	Host           string
	Topic          string
	GroupId        string
	ConnectTimeout time.Duration
	UseInsecureTLS bool
	Context        context.Context
	LineNumbers    bool
	Follow         bool
	Key            string
	Value          string
}

func Read(c *cli.Context) error {
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

	return k.NewReader("todo-some-id", opts).Read(opts.Context)
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

	return k.NewWriter("todo-some-id", opts).Write(opts.Key, opts.Value)
}

func parseOptions(c *cli.Context) (*Options, error) {
	if c.String("host") == "" {
		return nil, errors.New("host cannot be empty")
	}

	if c.String("topic") == "" {
		return nil, errors.New("topic cannot be empty")
	}

	return &Options{
		ConnectTimeout: c.Duration("timeout"),
		Topic:          c.String("topic"),
		Host:           c.String("host"),
		GroupId:        c.String("group-id"),
		UseInsecureTLS: c.Bool("insecure-tls"),
		Context:        context.Background(),
		Follow:         c.Bool("follow"),
		Key:            c.String("key"),
		Value:          c.String("value"),
		LineNumbers:    c.Bool("line-numbers"),
	}, nil
}

func (k *Kafka) NewReader(id string, opts *Options) *Reader {
	readerConfig := skafka.ReaderConfig{
		Brokers:       []string{opts.Host},
		GroupID:       opts.GroupId,
		Topic:         opts.Topic,
		Dialer:        k.Dialer,
		MaxWait:       DefaultMaxWait,
		MaxBytes:      DefaultMaxBytes,
		QueueCapacity: 1,
	}

	return &Reader{
		Id:      id,
		Options: opts,
		Reader:  skafka.NewReader(readerConfig),
		log:     logrus.WithField("readerID", id),
	}
}

func (r *Reader) Read(ctx context.Context) error {
	r.log.Info("Initializing...")

	lineNumbers := 1

	for {
		msg, err := r.Reader.ReadMessage(ctx)
		if err != nil {
			return errors.Wrap(err, "unable to read message")
		}

		if r.Options.LineNumbers {
			fmt.Printf("%d: %s\n", lineNumbers, string(msg.Value))
			lineNumbers++
		} else {
			fmt.Printf("%s\n", string(msg.Value))
		}

		if !r.Options.Follow {
			break
		}
	}

	r.log.Debug("Read complete")

	return nil
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
