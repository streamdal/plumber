package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/batchcorp/plumber/backends/kafka"
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
			Name:     "host",
			Usage:    "Destination host address (Example: localhost:9092)",
			Required: true,
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
										Name:  "type",
										Usage: "The type of message(s) you will receive on the bus",
										Value: &EnumValue{
											Enum:    []string{"plain", "json", "protobuf"},
											Default: "json",
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
										Name:  "output",
										Usage: "Convert received messages to output type",
										Value: &EnumValue{
											Enum:    []string{"base64"},
											Default: "",
										},
									},
								}...),
							},
							{
								Name:  "rabbitmq",
								Usage: "RabbitMQ message system",
								Action: func(c *cli.Context) error {
									fmt.Println("new task template: ", c.Args().First())
									return nil
								},
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
						Usage:   "What to read off of queue",
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
										Name:  "value",
										Usage: "Value to write to kafka",
									},
									&cli.GenericFlag{
										Name:  "output-type",
										Usage: "Convert the input to this type when writing message",
										Value: &EnumValue{
											Enum:    []string{"plain", "protobuf"},
											Default: "plain",
										},
									},
									&cli.GenericFlag{
										Name:  "input-type",
										Usage: "Treat input data as this type to enable output conversion",
										Value: &EnumValue{
											Enum:    []string{"plain", "base64", "jsonpb"},
											Default: "plain",
										},
									},
									&cli.StringFlag{
										Name:  "file",
										Usage: "File containing input data (1 file = 1 message)",
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
								Name:  "rabbitmq",
								Usage: "RabbitMQ message system",
								Action: func(c *cli.Context) error {
									fmt.Println("new task template: ", c.Args().First())
									return nil
								},
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
