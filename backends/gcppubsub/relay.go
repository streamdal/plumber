package gcppubsub

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	sdk "github.com/streamdal/streamdal/sdks/go"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/streamdal/plumber/backends/gcppubsub/types"
	"github.com/streamdal/plumber/util"

	"github.com/streamdal/plumber/prometheus"
	"github.com/streamdal/plumber/validate"
)

const RetryReadInterval = 5 * time.Second

func (g *GCPPubSub) Relay(ctx context.Context, relayOpts *opts.RelayOptions, relayCh chan interface{}, errorCh chan<- *records.ErrorRecord) error {
	if err := validateRelayOptions(relayOpts); err != nil {
		return errors.Wrap(err, "unable to validate relay options")
	}

	llog := g.log.WithFields(logrus.Fields{
		"relay-id": relayOpts.XRelayId,
		"backend":  "gcp-pubsub",
	})

	// streamdal sdk BEGIN
	sc, err := util.SetupStreamdalSDK(relayOpts, llog)
	if err != nil {
		return errors.Wrap(err, "kafka.Relay(): unable to create new streamdal client")
	}
	defer sc.Close()
	// streamdal sdk END

	var m sync.Mutex

	var readFunc = func(ctx context.Context, msg *pubsub.Message) {
		m.Lock()
		defer m.Unlock()

		if relayOpts.GcpPubsub.Args.AckMessages {
			defer msg.Ack()
		}

		prometheus.Incr("gcp-pubsub-relay-consumer", 1)

		// streamdal sdk BEGIN
		// If streamdal integration is enabled, process message via sdk
		if sc != nil {
			g.log.Debug("Processing message via streamdal SDK")

			resp := sc.Process(ctx, &sdk.ProcessRequest{
				ComponentName: "gcp-pubsub",
				OperationType: sdk.OperationTypeConsumer,
				OperationName: "relay",
				Data:          msg.Data,
			})

			if resp.Status == sdk.ExecStatusError {
				wrappedErr := fmt.Errorf("unable to process message via streamdal: %v", resp.StatusMessage)

				prometheus.IncrPromCounter("plumber_sdk_errors", 1)
				util.WriteError(llog, errorCh, wrappedErr)

				return
			}

			// Update msg value with processed data
			msg.Data = resp.Data
		}
		// streamdal sdk END

		g.log.Debug("Writing message to relay channel")

		relayCh <- &types.RelayMessage{
			Value:   msg,
			Options: &types.RelayMessageOptions{},
		}
	}

	sub := g.client.Subscription(relayOpts.GcpPubsub.Args.SubscriptionId)

	g.log.Infof("Relaying GCP pubsub messages from '%s' queue -> '%s'", sub.ID(), relayOpts.XStreamdalGrpcAddress)

MAIN:
	for {
		select {
		case <-ctx.Done():
			llog.Debug("detected context cancellation")
			break MAIN
		default:
			// NOOP
		}

		// sub.Receive() is not returning context.Canceled for some reason
		if err := sub.Receive(ctx, readFunc); err != nil {
			errorCh <- &records.ErrorRecord{
				Error:               errors.Wrap(err, "unable to relay GCP message").Error(),
				OccurredAtUnixTsUtc: time.Now().UTC().Unix(),
			}
			prometheus.Mute("gcp-pubsub-relay-consumer")
			prometheus.Mute("gcp-pubsub-relay-producer")

			prometheus.IncrPromCounter("plumber_read_errors", 1)

			g.log.WithField("err", err).Error("unable to read message(s) from GCP pubsub")
			time.Sleep(RetryReadInterval)
		}
	}

	llog.Debug("relay exiting")

	return nil
}

func validateRelayOptions(relayOpts *opts.RelayOptions) error {
	if relayOpts == nil {
		return validate.ErrEmptyRelayOpts
	}

	if relayOpts.GcpPubsub == nil {
		return validate.ErrEmptyBackendGroup
	}

	if relayOpts.GcpPubsub.Args == nil {
		return validate.ErrEmptyBackendArgs
	}

	if relayOpts.GcpPubsub.Args.SubscriptionId == "" {
		return errors.New("subscription ID cannot be empty")
	}

	return nil
}
