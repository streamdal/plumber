package mqtt

import (
	"fmt"
	"strings"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/reader"
)

// Read is the entry point function for performing read operations in MQTT.
//
// This is where we verify that the provided arguments and flag combination
// makes sense/are valid; this is also where we will perform our initial conn.
func Read(opts *cli.Options) error {
	if err := validateReadOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	var mdErr error
	var md *desc.MessageDescriptor

	if opts.ReadProtobufRootMessage != "" {
		md, mdErr = pb.FindMessageDescriptor(opts.ReadProtobufDirs, opts.ReadProtobufRootMessage)
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
		log:     logrus.WithField("pkg", "mqtt/read.go"),
	}

	return r.Read()
}

func (m *MQTT) Read() error {
	defer m.Client.Disconnect(0)

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
		data, err := reader.Decode(m.Options, m.MsgDesc, msg.Payload())

		if err != nil {
			if !m.Options.ReadFollow {
				errChan <- fmt.Errorf("unable to complete conversion: %s", err)
				wg.Done()
				return
			}

			return
		}

		str := string(data)

		if m.Options.ReadLineNumbers {
			str = fmt.Sprintf("%d: ", lineNumber) + str
			lineNumber++
		}

		printer.Print(str)

		if !m.Options.ReadFollow {
			m.log.Debug("--follow NOT specified, stopping listen")

			close(errChan)
			wg.Done()
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

	// If anything protobuf-related is specified, it's being used
	if opts.ReadProtobufRootMessage != "" || len(opts.ReadProtobufDirs) != 0 {
		if err := cli.ValidateProtobufOptions(
			opts.ReadProtobufDirs,
			opts.ReadProtobufRootMessage,
		); err != nil {
			return fmt.Errorf("unable to validate protobuf option(s): %s", err)
		}
	}

	return nil
}
