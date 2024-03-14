package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/backends/kafka/types"
	"github.com/streamdal/plumber/prometheus"
	"github.com/streamdal/plumber/util"
	"github.com/streamdal/plumber/validate"

	sdk "github.com/streamdal/streamdal/sdks/go"
)

const (
	RetryReadInterval = 5 * time.Second
)

// Relay sets up a new Kafka relayer
func (k *Kafka) Relay(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}, errorCh chan<- *records.ErrorRecord) error {
	if err := validateRelayOptions(relayOpts); err != nil {
		return errors.Wrap(err, "unable to verify options")
	}

	reader, err := NewReaderForRelay(k.dialer, k.connArgs, relayOpts.Kafka.Args)
	if err != nil {
		return errors.Wrap(err, "unable to create new reader")
	}

	defer reader.Close()

	// streamdal sdk BEGIN
	sc, err := util.SetupStreamdalSDK(relayOpts, k.log)
	if err != nil {
		return errors.Wrap(err, "kafka.Relay(): unable to create new streamdal client")
	}
	// TODO: go-sdk needs to support sc.Close() so we can defer
	//
	// streamdal sdk END

	llog := k.log.WithFields(logrus.Fields{
		"relay-id": relayOpts.XRelayId,
		"backend":  "kafka",
		"function": "Relay",
	})

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			// Shutdown cancelled, exit so we don't spam logs with context cancelled errors
			if err == context.Canceled {
				llog.Debug("Received shutdown signal, exiting relayer")
				break
			}

			prometheus.Mute("kafka-relay-consumer")
			prometheus.Mute("kafka-relay-producer")

			prometheus.IncrPromCounter("plumber_read_errors", 1)

			wrappedErr := fmt.Errorf("unable to read kafka message: %s; retrying in %s", err, RetryReadInterval)
			util.WriteError(llog, errorCh, wrappedErr)

			time.Sleep(RetryReadInterval)

			continue
		}

		prometheus.Incr("kafka-relay-consumer", 1)

		// streamdal sdk BEGIN
		// If streamdal integration is enabled, process message via sdk
		if sc != nil {
			k.log.Debug("Processing message via streamdal SDK")

			resp := sc.Process(ctx, &sdk.ProcessRequest{
				ComponentName: "kafka",
				OperationType: sdk.OperationTypeConsumer,
				OperationName: "relay",
				Data:          msg.Value,
			})

			if resp.Status == sdk.ExecStatusError {
				wrappedErr := fmt.Errorf("unable to process message via streamdal: %v", resp.StatusMessage)

				prometheus.IncrPromCounter("plumber_sdk_errors", 1)
				util.WriteError(llog, errorCh, wrappedErr)

				continue
			}

			// Update msg value with processed data
			msg.Value = resp.Data
		}
		// streamdal sdk END

		k.log.Debugf("Writing Kafka message to relay channel: %s", msg.Value)

		relayCh <- &types.RelayMessage{
			Value:   &msg,
			Options: &types.RelayMessageOptions{},
		}
	}

	llog.Debug("relay exiting")

	return nil
}

// validateRelayOptions ensures all required relay options are present
func validateRelayOptions(relayOpts *opts.RelayOptions) error {
	if relayOpts == nil {
		return validate.ErrEmptyRelayOpts
	}

	if relayOpts.Kafka == nil {
		return validate.ErrEmptyBackendGroup
	}

	if relayOpts.Kafka.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if len(relayOpts.Kafka.Args.Topics) == 0 {
		return ErrMissingTopic
	}

	return nil
}
