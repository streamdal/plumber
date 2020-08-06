package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

const (
	DefaultConnectTimeout = "10s"
	DefaultGroupId        = "plumber"
	DefaultMaxBytes       = 1048576 // 1MB
	DefaultMaxWait        = 50 * time.Millisecond
	DefaultBatchSize      = 1
)

// Kafka holds all attributes required for performing a write to Kafka. This
// struct should be instantiated via the kafka.Read(..) func.
type Kafka struct {
	Id          string
	Reader      *skafka.Reader
	Writer      *skafka.Writer
	Options     *Options
	MessageDesc *desc.MessageDescriptor
	log         *logrus.Entry
}

// Options contains the values parsed from urfave args and flags. This struct
// gets filled out by helper parse* func(s).
type Options struct {
	Address             string
	Topic               string
	GroupId             string
	ConnectTimeout      time.Duration
	UseInsecureTLS      bool
	Context             context.Context
	LineNumbers         bool
	Follow              bool
	Key                 string
	InputData           string
	ProtobufDir         string
	ProtobufRootMessage string
	InputType           string
	OutputType          string
	InputFile           string
	Convert             string
}

func NewReader(opts *Options) (*skafka.Reader, error) {
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
	if _, err := dialer.DialLeader(ctxDeadline, "tcp", opts.Address, opts.Topic, 0); err != nil {
		return nil, fmt.Errorf("unable to create initial connection to host '%s': %s",
			opts.Address, err)
	}

	return skafka.NewReader(skafka.ReaderConfig{
		Brokers:       []string{opts.Address},
		GroupID:       opts.GroupId,
		Topic:         opts.Topic,
		Dialer:        dialer,
		MaxWait:       DefaultMaxWait,
		MaxBytes:      DefaultMaxBytes,
		QueueCapacity: 1,
	}), nil
}

func NewWriter(opts *Options) (*skafka.Writer, error) {
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
	if _, err := dialer.DialLeader(ctxDeadline, "tcp", opts.Address, opts.Topic, 0); err != nil {
		return nil, fmt.Errorf("unable to create initial connection to host '%s': %s",
			opts.Address, err)
	}

	return skafka.NewWriter(skafka.WriterConfig{
		Brokers:   []string{opts.Address},
		Topic:     opts.Topic,
		Dialer:    dialer,
		BatchSize: DefaultBatchSize,
	}), nil
}

func parseOptions(c *cli.Context) (*Options, error) {
	if c.String("address") == "" {
		return nil, errors.New("--address cannot be empty")
	}

	if c.String("topic") == "" {
		return nil, errors.New("--topic cannot be empty")
	}

	return &Options{
		ConnectTimeout:      c.Duration("timeout"),
		Topic:               c.String("topic"),
		Address:             c.String("address"),
		GroupId:             c.String("group-id"),
		UseInsecureTLS:      c.Bool("insecure-tls"),
		Context:             context.Background(),
		Follow:              c.Bool("follow"),
		Key:                 c.String("key"),
		LineNumbers:         c.Bool("line-numbers"),
		ProtobufDir:         c.String("protobuf-dir"),
		ProtobufRootMessage: c.String("protobuf-root-message"),
		InputType:           c.String("input-type"),
		InputData:           c.String("input-data"),
		InputFile:           c.String("input-file"),
		OutputType:          c.String("output-type"),
		Convert:             c.String("convert"),
	}, nil
}
