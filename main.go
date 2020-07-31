package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/batchcorp/plumber/backends/kafka"
	"github.com/batchcorp/plumber/backends/rabbitmq"
)

var (
	Version = "UNSET"
)

func main() {
	app := setupCLI()

	if err := app.Run(os.Args); err != nil {
		logrus.Fatalf("Unable to complete '%s' run: %s", os.Args, err)
	}
}

type EnumValue struct {
	Enum     []string
	Default  string
	selected string
}

func (e *EnumValue) Set(value string) error {
	for _, enum := range e.Enum {
		if enum == value {
			e.selected = value
			return nil
		}
	}

	return fmt.Errorf("allowed values are %s", strings.Join(e.Enum, ", "))
}

func (e EnumValue) String() string {
	if e.selected == "" {
		return e.Default
	}
	return e.selected
}

func setupCLI() *cli.App {
	cli.VersionFlag = &cli.BoolFlag{
		Name:    "version",
		Usage:   "Print the version of plumber",
		Aliases: []string{"v"},
	}

	kafkaFlags := []cli.Flag{
		&cli.StringFlag{
			Name:  "address",
			Usage: "Destination host address",
			Value: "localhost:9092",
		},
		&cli.StringFlag{
			Name:     "topic",
			Usage:    "Topic to read messages from",
			Required: true,
		},
		&cli.DurationFlag{
			Name:  "timeout",
			Usage: "How long we should attempt to connect for",
			Value: kafka.DefaultConnectTimeout,
		},
		&cli.BoolFlag{
			Name:  "insecure-tls",
			Usage: "Whether to use insecure-tls",
			Value: false,
		},
		&cli.BoolFlag{Name: "line-numbers"},
	}

	rabbitmqFlags := []cli.Flag{
		&cli.StringFlag{
			Name:  "address",
			Usage: "Destination host address",
			Value: "amqp://localhost",
		},
		&cli.StringFlag{
			Name:  "exchange",
			Usage: "Name of the exchange (leave empty if default)",
		},
		&cli.StringFlag{
			Name:     "routing-key",
			Usage:    "Routing key",
			Required: true,
		},
	}

	globalFlags := []cli.Flag{
		&cli.BoolFlag{
			Name:    "debug",
			Aliases: []string{"d"},
			Usage:   "Enable debug output",
			Value:   false,
		},
	}

	app := &cli.App{
		Name:    "plumber",
		Version: Version,
		Commands: []*cli.Command{
			{
				Name:  "read",
				Usage: "Read messages from messaging system",
				Subcommands: []*cli.Command{
					{
						Name:    "message",
						Aliases: []string{"messages"},
						Usage:   "What to read off of queue",
						Subcommands: []*cli.Command{
							{
								Name:   "kafka",
								Usage:  "Kafka message system",
								Action: kafka.Read,
								Flags: append(kafkaFlags, []cli.Flag{
									&cli.StringFlag{
										Name:  "group-id",
										Usage: "Specify a specific group-id to use when reading from kafka",
										Value: kafka.DefaultGroupId,
									},
									&cli.BoolFlag{
										Name:    "follow",
										Aliases: []string{"f"},
									},
									&cli.GenericFlag{
										Name:  "output-type",
										Usage: "The type of message(s) you will receive on the bus",
										Value: &EnumValue{
											Enum:    []string{"plain", "protobuf"},
											Default: "plain",
										},
									},
									&cli.StringFlag{
										Name:  "protobuf-dir",
										Usage: "Directory with .proto files",
									},
									&cli.StringFlag{
										Name:  "protobuf-root-message",
										Usage: "Specifies the root message in a protobuf descriptor set (required if protobuf-dir set)",
									},
									&cli.GenericFlag{
										Name:  "convert",
										Usage: "Convert messages received on the bus",
										Value: &EnumValue{
											Enum:    []string{"base64", "gzip"},
											Default: "",
										},
									},
								}...),
							},
							{
								Name:   "rabbitmq",
								Usage:  "RabbitMQ message system",
								Action: rabbitmq.Read,
								Flags: append(rabbitmqFlags, []cli.Flag{
									&cli.StringFlag{
										Name:  "queue",
										Usage: "Name of the queue where messages will be routed to",
									},
									&cli.BoolFlag{
										Name:  "queue-durable",
										Usage: "Whether the created queue will remain after restart",
										Value: false,
									},
									&cli.BoolFlag{
										Name:  "queue-auto-delete",
										Usage: "Whether to auto-delete the queue after plumber has disconnected",
										Value: true,
									},
									&cli.BoolFlag{
										Name:  "queue-exclusive",
										Usage: "Whether plumber should be the only one using the newly defined queue",
										Value: true,
									},
									&cli.BoolFlag{
										Name:  "line-numbers",
										Usage: "Display line numbers for each message",
									},
									&cli.BoolFlag{
										Name:  "follow",
										Usage: "Continue reading until cancelled (like tail -f)",
									},
									&cli.StringFlag{
										Name:  "protobuf-dir",
										Usage: "Directory with .proto files",
									},
									&cli.StringFlag{
										Name:  "protobuf-root-message",
										Usage: "Specifies the root message in a protobuf descriptor set (required if protobuf-dir set)",
									},
									&cli.GenericFlag{
										Name:  "output-type",
										Usage: "The type of message(s) you will receive on the bus",
										Value: &EnumValue{
											Enum:    []string{"plain", "protobuf"},
											Default: "plain",
										},
									},
									&cli.GenericFlag{
										Name:  "convert",
										Usage: "Convert messages received on the bus",
										Value: &EnumValue{
											Enum:    []string{"base64", "gzip"},
											Default: "",
										},
									},
								}...),
							},
						},
					},
				},
			},
			{
				Name:  "write",
				Usage: "Write message(s) to messaging system",
				Subcommands: []*cli.Command{
					{
						Name:    "message",
						Aliases: []string{"messages"},
						Usage:   "What to write to queue",
						Subcommands: []*cli.Command{
							{
								Name:   "kafka",
								Usage:  "Kafka message system",
								Action: kafka.Write,
								Flags: append(kafkaFlags, []cli.Flag{
									&cli.StringFlag{
										Name:  "key",
										Usage: "Key to write to kafka",
										Value: "plumber-default-key",
									},
									&cli.StringFlag{
										Name:  "input-data",
										Usage: "The data to write to kafka",
									},
									&cli.StringFlag{
										Name:  "input-file",
										Usage: "File containing input data (1 file = 1 message)",
									},
									&cli.GenericFlag{
										Name:  "input-type",
										Usage: "Treat input data as this type to enable output conversion",
										Value: &EnumValue{
											Enum:    []string{"plain", "base64", "jsonpb"},
											Default: "plain",
										},
									},
									&cli.GenericFlag{
										Name:  "output-type",
										Usage: "Convert the input to this type when writing message",
										Value: &EnumValue{
											Enum:    []string{"plain", "protobuf"},
											Default: "plain",
										},
									},
									&cli.StringFlag{
										Name:  "protobuf-dir",
										Usage: "Directory with .proto files",
									},
									&cli.StringFlag{
										Name:  "protobuf-root-message",
										Usage: "Specifies the root message in a protobuf descriptor set (required if protobuf-dir set)",
									},
								}...),
							},
							{
								Name:   "rabbitmq",
								Usage:  "RabbitMQ message system",
								Action: rabbitmq.Write,
								Flags: append(rabbitmqFlags, []cli.Flag{
									&cli.StringFlag{
										Name:  "key",
										Usage: "Key to write to rabbitmq",
										Value: "plumber-default-key",
									},
									&cli.StringFlag{
										Name:  "input-data",
										Usage: "The data to write to rabbitmq",
									},
									&cli.StringFlag{
										Name:  "input-file",
										Usage: "File containing input data (1 file = 1 message)",
									},
									&cli.GenericFlag{
										Name:  "input-type",
										Usage: "Treat input data as this type to enable output conversion",
										Value: &EnumValue{
											Enum:    []string{"plain", "base64", "jsonpb"},
											Default: "plain",
										},
									},
									&cli.GenericFlag{
										Name:  "output-type",
										Usage: "Convert the input to this type when writing message",
										Value: &EnumValue{
											Enum:    []string{"plain", "protobuf"},
											Default: "plain",
										},
									},
									&cli.StringFlag{
										Name:  "protobuf-dir",
										Usage: "Directory with .proto files",
									},
									&cli.StringFlag{
										Name:  "protobuf-root-message",
										Usage: "Specifies the root message in a protobuf descriptor set (required if protobuf-dir set)",
									},
								}...),
							},
						},
					},
				},
			},
		},
		Flags: globalFlags,
		Before: func(c *cli.Context) error {
			if c.Bool("debug") {
				logrus.SetLevel(logrus.DebugLevel)

				logrus.Debug("Enabled debug mode")
			}

			return nil
		},
	}

	return app
}
