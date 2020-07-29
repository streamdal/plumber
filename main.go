package main

import (
	"fmt"
	"log"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/batchcorp/plumber/backends/kafka"
)

var (
	Version = "N/A"
)

func main() {
	kafkaFlags := []cli.Flag{
		&cli.StringFlag{
			Name:     "host",
			Usage:    "Destination host address (Example: localhost:9042)",
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

	app := &cli.App{
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
										Value: kafka.DefaultGroupId, // TODO: Concat UUID string
									},
									&cli.BoolFlag{
										Name:    "follow",
										Aliases: []string{"f"},
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
										Name:     "value",
										Usage:    "Value to write to kafka",
										Required: true,
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
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
