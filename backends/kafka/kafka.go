package kafka

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
)

const (
	DefaultConnectTimeout = 10 * time.Second
	DefaultGroupId        = "plumber"
	DefaultMaxBytes       = 1048576 // 1MB
	DefaultMaxWait        = 50 * time.Millisecond
	DefaultBatchSize      = 1
)

// Options contains the values parsed from urfave args and flags. This struct
// gets filled out by helper parse* func(s).
//
// TODO: This should be populated by urfave at startup.
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
