package mqtt

import (
	"encoding/base64"
	"fmt"
	"strings"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/serializers"
	"github.com/batchcorp/plumber/util"
)

// Read is the entry point function for performing read operations in RabbitMQ.
//
// This is where we verify that the provided arguments and flag combination
// makes sense/are valid; this is also where we will perform our initial conn.
func Read(opts *cli.Options) error {
	if err := validateReadOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	var mdErr error
	var md *desc.MessageDescriptor

	if opts.MQTT.ReadOutputType == "protobuf" {
		md, mdErr = pb.FindMessageDescriptor(opts.MQTT.ReadProtobufDirs, opts.MQTT.ReadProtobufRootMessage)
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
		log:     logrus.WithField("pkg", "rabbitmq/read.go"),
	}

	return r.Read()
}

func (m *MQTT) Read() error {
	m.log.Infof("Listening for message(s) on topic '%s' as clientId '%s'",
		m.Options.MQTT.Topic, m.Options.MQTT.ClientId)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	errChan := make(chan error, 1)

	m.subscribe(wg, errChan)

	wg.Wait()

	err, closed := <-errChan
	if closed {
		return nil
	}

	return err
}

func (m *MQTT) subscribe(wg *sync.WaitGroup, errChan chan error) {
	lineNumber := 1

	m.Client.Subscribe(m.Options.MQTT.Topic, 0, func(client mqtt.Client, msg mqtt.Message) {
		if !m.Options.MQTT.ReadFollow {
			defer m.Client.Disconnect(0)
		}

		msgData := msg.Payload()

		if m.Options.MQTT.ReadOutputType == "protobuf" {
			decoded, err := pb.DecodeProtobufToJSON(dynamic.NewMessage(m.MsgDesc), msg.Payload())
			if err != nil {
				if !m.Options.MQTT.ReadFollow {
					errChan <- fmt.Errorf("unable to decode protobuf message: %s", err)
					wg.Done()
					return
				}

				printer.Error(fmt.Sprintf("unable to decode protobuf message: %s", err))
				return
			}

			msgData = decoded
		}

		// Handle AVRO
		if m.Options.AvroSchemaFile != "" {
			decoded, err := serializers.AvroDecode(m.Options.AvroSchemaFile, msgData)
			if err != nil {
				printer.Error(fmt.Sprintf("unable to decode AVRO message: %s", err))
				return
			}
			msgData = decoded
		}

		var data []byte
		var convertErr error

		switch m.Options.MQTT.ReadConvert {
		case "base64":
			_, convertErr = base64.StdEncoding.Decode(data, msgData)
		case "gzip":
			data, convertErr = util.Gunzip(msgData)
		default:
			data = msgData
		}

		if convertErr != nil {
			if !m.Options.MQTT.ReadFollow {
				errChan <- fmt.Errorf("unable to complete conversion: %s", convertErr)
				wg.Done()
				return
			}

			printer.Error(fmt.Sprintf("unable to complete conversion for message: %s", convertErr))
			return
		}

		str := string(data)

		if m.Options.MQTT.LineNumbers {
			str = fmt.Sprintf("%d: ", lineNumber) + str
			lineNumber++
		}

		printer.Print(str)

		if !m.Options.MQTT.ReadFollow {
			m.log.Debug("--follow NOT specified, stopping listen")

			close(errChan)
			wg.Done()
			m.Client.Disconnect(0)
		}
	})
}

func validateReadOptions(opts *cli.Options) error {
	if opts.MQTT.Address == "" {
		return errors.New("--address cannot be empty")
	}

	if opts.MQTT.Topic == "" {
		return errors.New("--topic cannot be empty")
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

	if opts.MQTT.QoSLevel > 2 || opts.MQTT.QoSLevel < 0 {
		return errors.New("QoS level can only be 0, 1 or 2")
	}

	if opts.MQTT.ReadOutputType == "protobuf" {
		if err := cli.ValidateProtobufOptions(
			opts.MQTT.ReadProtobufDirs,
			opts.MQTT.ReadProtobufRootMessage,
		); err != nil {
			return fmt.Errorf("unable to validate protobuf option(s): %s", err)
		}
	}

	return nil
}
