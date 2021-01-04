package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"time"

	"github.com/jhump/protoreflect/desc"
	skafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh/terminal"

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
	MsgDesc *desc.MessageDescriptor
	log     *logrus.Entry
}

type KafkaReader struct {
	Reader *skafka.Reader
	Conn   *skafka.Conn
}

type KafkaWriter struct {
	Writer *skafka.Writer
	Conn   *skafka.Conn
}

func NewReader(opts *cli.Options) (*KafkaReader, error) {
	dialer := &skafka.Dialer{
		Timeout: opts.Kafka.Timeout,
	}

	if opts.Kafka.InsecureTLS {
		dialer.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	auth, err := getAuthenticationMechanism(opts)
	if err != nil {
		return nil, fmt.Errorf("unable to create initial connection to host '%s': %s",
			opts.Kafka.Address, err)
	}

	dialer.SASLMechanism = auth

	// The dialer timeout does not get utilized under some conditions (such as
	// when kafka is configured to NOT auto create topics) - we need a
	// mechanism to bail out early.
	ctxDeadline, _ := context.WithDeadline(context.Background(), time.Now().Add(opts.Kafka.Timeout))

	// Attempt to establish connection on startup
	conn, err := dialer.DialLeader(ctxDeadline, "tcp", opts.Kafka.Address, opts.Kafka.Topic, 0)
	if err != nil {
		return nil, fmt.Errorf("unable to create initial connection to host '%s': %s",
			opts.Kafka.Address, err)
	}

	r := skafka.NewReader(skafka.ReaderConfig{
		Brokers:       []string{opts.Kafka.Address},
		GroupID:       opts.Kafka.ReadGroupId,
		Topic:         opts.Kafka.Topic,
		Dialer:        dialer,
		MaxWait:       DefaultMaxWait,
		MaxBytes:      DefaultMaxBytes,
		QueueCapacity: 1,
	})

	return &KafkaReader{
		Reader: r,
		Conn:   conn,
	}, nil
}

func NewWriter(opts *cli.Options) (*KafkaWriter, error) {
	dialer := &skafka.Dialer{
		Timeout: opts.Kafka.Timeout,
	}

	if opts.Kafka.InsecureTLS {
		dialer.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	auth, err := getAuthenticationMechanism(opts)
	if err != nil {
		return nil, fmt.Errorf("unable to create initial connection to host '%s': %s",
			opts.Kafka.Address, err)
	}

	dialer.SASLMechanism = auth

	// The dialer timeout does not get utilized under some conditions (such as
	// when kafka is configured to NOT auto create topics) - we need a
	// mechanism to bail out early.
	ctxDeadline, _ := context.WithDeadline(context.Background(), time.Now().Add(opts.Kafka.Timeout))

	// Attempt to establish connection on startup
	conn, err := dialer.DialLeader(ctxDeadline, "tcp", opts.Kafka.Address, opts.Kafka.Topic, 0)
	if err != nil {
		return nil, fmt.Errorf("unable to create initial connection to host '%s': %s",
			opts.Kafka.Address, err)
	}

	w := skafka.NewWriter(skafka.WriterConfig{
		Brokers:   []string{opts.Kafka.Address},
		Topic:     opts.Kafka.Topic,
		Dialer:    dialer,
		BatchSize: DefaultBatchSize,
	})

	return &KafkaWriter{
		Writer: w,
		Conn:   conn,
	}, nil
}

// getAuthenticationMechanism returns the correct authentication config for use with kafka.Dialer if a username/password
// is provided. If not, it will return nil
func getAuthenticationMechanism(opts *cli.Options) (sasl.Mechanism, error) {
	if opts.Kafka.Username == "" {
		return nil, nil
	}

	// Username given, but no password. Prompt user for it
	if opts.Kafka.Password == "" {
		password, err := readPassword()
		if err != nil {
			return nil, err
		}
		opts.Kafka.Password = password
	}

	switch opts.Kafka.AuthenticationType {
	case "scram":
		return scram.Mechanism(scram.SHA512, opts.Kafka.Username, opts.Kafka.Password)
	default:
		return plain.Mechanism{
			Username: opts.Kafka.Username,
			Password: opts.Kafka.Password,
		}, nil
	}
}

// readPassword prompts the user for a password from stdin
func readPassword() (string, error) {
	for {
		fmt.Print("Enter Password: ")

		// int typecast is needed for windows
		password, err := terminal.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return "", errors.New("you must enter a password")
		}

		fmt.Println("")

		sp := strings.TrimSpace(string(password))
		if sp != "" {
			return sp, nil
		}
	}
}
