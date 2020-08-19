package mqtt

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
)

// Write is the entry point function for performing write operations in MQTT.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func Write(opts *cli.Options) error {
	if err := validateWriteOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	var mdErr error
	var md *desc.MessageDescriptor

	if opts.MQTT.WriteOutputType == "protobuf" {
		md, mdErr = pb.FindMessageDescriptor(opts.MQTT.WriteProtobufDir, opts.MQTT.WriteProtobufRootMessage)
		if mdErr != nil {
			return errors.Wrap(mdErr, "unable to find root message descriptor")
		}
	}

	client, err := connect(opts)
	if err != nil {
		return errors.Wrap(err, "unable to complete initial connect")
	}

	r := &MQTT{
		Options: opts,
		Client:  client,
		MsgDesc: md,
		log:     logrus.WithField("pkg", "rabbitmq/write.go"),
	}

	msg, err := generateWriteValue(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	return r.Write(msg)
}

// Write is a wrapper for amqp Publish method. We wrap it so that we can mock
// it in tests, add logging etc.
func (m *MQTT) Write(value []byte) error {
	m.log.Infof("Sending message to broker on topic '%s' as clientId '%s'",
		m.Options.MQTT.Topic, m.Options.MQTT.ClientId)

	token := m.Client.Publish(m.Options.MQTT.Topic, byte(m.Options.MQTT.QoSLevel), false, value)

	if !token.WaitTimeout(m.Options.MQTT.WriteTimeout) {
		return fmt.Errorf("timed out attempting to publish message after %s", m.Options.MQTT.WriteTimeout)
	}

	if token.Error() != nil {
		return errors.Wrap(token.Error(), "unable to complete publish")
	}

	return nil
}

func validateWriteOptions(opts *cli.Options) error {
	if opts.MQTT.QoSLevel > 2 || opts.MQTT.QoSLevel < 0 {
		return errors.New("QoS level can only be 0, 1 or 2")
	}

	if strings.HasPrefix(opts.MQTT.Address, "ssl") {
		if opts.MQTT.TLSClientKeyFile == "" {
			return errors.New("--tls-client-key-file cannot be blank if using ssl")
		}

		if opts.MQTT.TLSClientCertFile == "" {
			return errors.New("--tls-client-cert-file cannot be blank if using ssl")
		}

		if opts.MQTT.TLSCAFile == "" {
			return errors.New("--tls-ca-file cannot be blank if using ssl")
		}
	}

	// If output-type is protobuf, ensure that protobuf flags are set
	// If type is protobuf, ensure both --protobuf-dir and --protobuf-root-message
	// are set as well
	if opts.MQTT.WriteOutputType == "protobuf" {
		if opts.MQTT.WriteProtobufDir == "" {
			return errors.New("'--protobuf-dir' must be set when type " +
				"is set to 'protobuf'")
		}

		if opts.MQTT.WriteProtobufRootMessage == "" {
			return errors.New("'--protobuf-root-message' must be when " +
				"type is set to 'protobuf'")
		}

		// Does given dir exist?
		if _, err := os.Stat(opts.MQTT.WriteProtobufDir); os.IsNotExist(err) {
			return fmt.Errorf("--protobuf-dir '%s' does not exist", opts.MQTT.WriteProtobufDir)
		}
	}

	// InputData and file cannot be set at the same time
	if opts.MQTT.WriteInputData != "" && opts.MQTT.WriteInputFile != "" {
		return fmt.Errorf("--input-data and --input-file cannot both be set (choose one!)")
	}

	if opts.MQTT.WriteInputFile != "" {
		if _, err := os.Stat(opts.MQTT.WriteInputFile); os.IsNotExist(err) {
			return fmt.Errorf("--input-file '%s' does not exist", opts.MQTT.WriteInputFile)
		}
	}

	return nil
}

func generateWriteValue(md *desc.MessageDescriptor, opts *cli.Options) ([]byte, error) {
	// Do we read value or file?
	var data []byte

	if opts.MQTT.WriteInputData != "" {
		data = []byte(opts.MQTT.WriteInputData)
	}

	if opts.MQTT.WriteInputFile != "" {
		var readErr error

		data, readErr = ioutil.ReadFile(opts.MQTT.WriteInputFile)
		if readErr != nil {
			return nil, fmt.Errorf("unable to read file '%s': %s", opts.MQTT.WriteInputFile, readErr)
		}
	}

	// Ensure we do not try to operate on a nil md
	if opts.MQTT.WriteOutputType == "protobuf" && md == nil {
		return nil, errors.New("message descriptor cannot be nil when --output-type is protobuf")
	}

	// Input: Plain Output: Plain
	if opts.MQTT.WriteInputType == "plain" && opts.MQTT.WriteOutputType == "plain" {
		return data, nil
	}

	// Input: JSONPB Output: Protobuf
	if opts.MQTT.WriteInputType == "jsonpb" && opts.MQTT.WriteOutputType == "protobuf" {
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

	// Now let's encode that into a proper protobuf message
	pbBytes, err := proto.Marshal(m)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal dynamic protobuf message to bytes")
	}

	return pbBytes, nil
}
