package kafka

import (
	"context"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
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

type Kafka struct {
	Options           *Options
	Dialer            *skafka.Dialer
	MessageDescriptor *desc.MessageDescriptor
}

type Options struct {
	Host                string
	Topic               string
	GroupId             string
	ConnectTimeout      time.Duration
	UseInsecureTLS      bool
	Context             context.Context
	LineNumbers         bool
	Follow              bool
	Key                 string
	Value               string
	Type                string
	ProtobufDir         string
	ProtobufRootMessage string
	Output              string
}

func parseOptions(c *cli.Context) (*Options, error) {
	if c.String("host") == "" {
		return nil, errors.New("host cannot be empty")
	}

	if c.String("topic") == "" {
		return nil, errors.New("topic cannot be empty")
	}

	return &Options{
		ConnectTimeout:      c.Duration("timeout"),
		Topic:               c.String("topic"),
		Host:                c.String("host"),
		GroupId:             c.String("group-id"),
		UseInsecureTLS:      c.Bool("insecure-tls"),
		Context:             context.Background(),
		Follow:              c.Bool("follow"),
		Key:                 c.String("key"),
		Value:               c.String("value"),
		LineNumbers:         c.Bool("line-numbers"),
		Type:                c.String("type"),
		ProtobufDir:         c.String("protobuf-dir"),
		ProtobufRootMessage: c.String("protobuf-root-message"),
		Output:              c.String("output"),
	}, nil
}
