package kafka

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
	skafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/batchcorp/plumber/pb"
)

type IWriter interface {
	Write(key, value []byte) error
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

	if err := validateWriteOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	var mdErr error
	var md *desc.MessageDescriptor

	if opts.OutputType == "protobuf" {
		md, mdErr = pb.FindMessageDescriptor(opts.ProtobufDir, opts.ProtobufRootMessage)
		if mdErr != nil {
			return errors.Wrap(mdErr, "unable to find root message descriptor")
		}
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

	value, err := generateWriteValue(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	return k.NewWriter("plumber-writer", opts).Write([]byte(opts.Key), value)
}

func generateWriteValue(md *desc.MessageDescriptor, opts *Options) ([]byte, error) {
	// Do we read value or file?
	var data []byte

	if opts.InputData != "" {
		data = []byte(opts.InputData)
	}

	if opts.InputFile != "" {
		var readErr error

		data, readErr = ioutil.ReadFile(opts.InputFile)
		if readErr != nil {
			return nil, fmt.Errorf("unable to read file '%s': %s", opts.InputFile, readErr)
		}
	}

	// Ensure we do not try to operate on a nil md
	if opts.OutputType == "protobuf" && md == nil {
		return nil, errors.New("message descriptor cannot be nil when --output-type is protobuf")
	}

	// Input: Plain Output: Plain
	if opts.InputType == "plain" && opts.OutputType == "plain" {
		return data, nil
	}

	// Input: JSONPB Output: Protobuf
	if opts.InputType == "jsonpb" && opts.OutputType == "protobuf" {
		var convertErr error

		data, convertErr = convertJSONPBToProtobuf(data, dynamic.NewMessage(md))
		if convertErr != nil {
			return nil, errors.Wrap(convertErr, "unable to convert JSONPB to protobuf")
		}

		return data, nil
	}

	// TODO: Input: Base64 Output: Plain
	// TODO: Input: Base64 Output: Protobuf
	// TODO: And a few more combinations ...

	return nil, errors.New("unsupported input/output combination")
}

// Convert jsonpb -> protobuf -> bytes
func convertJSONPBToProtobuf(data []byte, m *dynamic.Message) ([]byte, error) {
	buf := bytes.NewBuffer(data)

	if err := jsonpb.Unmarshal(buf, m); err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal data into dynamic message")
	}

	fmt.Printf("This is what our marshalled dynamic message looks like: %+v\n", m)

	// Now let's encode that into a proper protobuf message
	pbBytes, err := proto.Marshal(m)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal dynamic protobuf message to bytes")
	}

	return pbBytes, nil
}

func validateWriteOptions(opts *Options) error {
	// If output-type is protobuf, ensure that protobuf flags are set
	// If type is protobuf, ensure both --protobuf-dir and --protobuf-root-message
	// are set as well
	if opts.OutputType == "protobuf" {
		if opts.ProtobufDir == "" {
			return errors.New("'protobuf-dir' must be set when type " +
				"is set to 'protobuf'")
		}

		if opts.ProtobufRootMessage == "" {
			return errors.New("'protobuf-root-message' must be when " +
				"type is set to 'protobuf'")
		}

		// Does given dir exist?
		if _, err := os.Stat(opts.ProtobufDir); os.IsNotExist(err) {
			return fmt.Errorf("protobuf-dir '%s' does not exist", opts.ProtobufDir)
		}
	}

	// InputData and file cannot be set at the same time
	if opts.InputData != "" && opts.InputFile != "" {
		return fmt.Errorf("--value and --file cannot both be set")
	}

	if opts.InputFile != "" {
		if _, err := os.Stat(opts.InputFile); os.IsNotExist(err) {
			return fmt.Errorf("--file '%s' does not exist", opts.InputFile)
		}
	}

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
func (w *Writer) Write(key, value []byte) error {
	if err := w.Writer.WriteMessages(w.Options.Context, skafka.Message{
		Key:   key,
		Value: value,
	}); err != nil {
		return errors.Wrap(err, "unable to publish message(s)")
	}

	return nil
}
