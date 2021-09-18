package types

import (
	"context"
	"fmt"
	"time"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber/backends/kafka"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/relay"
)

type Relay struct {
	Active     bool                `json:"-"`
	Id         string              `json:"-"`
	CancelCtx  context.Context     `json:"-"`
	CancelFunc context.CancelFunc  `json:"-"`
	RelayCh    chan interface{}    `json:"-"`
	Backend    relay.IRelayBackend `json:"-"`
	Options    *opts.RelayOptions  `json:"config"`
	log        *logrus.Entry       `json:"-"`
}

// StartRelay starts a configured relay, it's workers, and the GRPC workers
func (r *Relay) StartRelay(conn *opts.ConnectionOptions) error {

	relayCh := make(chan interface{})

	// Needed to satisfy relay.Options{}, not used
	_, stubCancelFunc := context.WithCancel(context.Background())

	rr, relayType, err := getRelayBackend(r.Options, conn, relayCh, r.CancelCtx)
	if err != nil {
		stubCancelFunc()
		return errors.Wrap(err, "unable to get backend for relay")
	}

	relayCfg := &relay.Config{
		Token:              r.Options.CollectionToken,
		GRPCAddress:        r.Options.XBatchshGrpcAddress,
		NumWorkers:         5,
		Timeout:            util.DurationSec(r.Options.XBatchshGrpcTimeoutSeconds),
		RelayCh:            relayCh,
		DisableTLS:         r.Options.XBatchshGrpcDisableTls,
		BatchSize:          r.Options.BatchSize,
		Type:               relayType,
		MainShutdownFunc:   stubCancelFunc,
		ServiceShutdownCtx: r.CancelCtx,
	}

	grpcRelayer, err := relay.New(relayCfg)
	if err != nil {
		return errors.Wrap(err, "unable to create new relayer instance")
	}

	// Launch gRPC Workers
	if err := grpcRelayer.StartWorkers(r.CancelCtx); err != nil {
		return errors.Wrap(err, "unable to start gRPC relay workers")
	}

	// TODO: The relay needs to be ran in a goroutine so it continues in the background, but we
	// TODO: need to check if it errors on startup somehow. Maybe move reader initialization outside of kafka.Relay()?
	go func() {
		// TODO: channel to return error and sit and wait for a while before returning CreateRelayResponse
		if err := rr.Relay(); err != nil {
			return
		}
	}()

	r.Backend = rr

	return nil
}

// TODO: This needs to go - no longer needed
func getRelayBackend(
	req *protos.Relay,
	conn *opts.ConnectionOptions,
	relayCh chan interface{},
	shutdownCtx context.Context,
) (rr relay.IRelayBackend, relayType string, err error) {

	switch {
	case req.GetKafka() != nil:
		args := req.GetKafka()
		cfg := conn.GetKafka()
		relayType = "kafka"

		commitInterval, err := time.ParseDuration(fmt.Sprintf("%ds", args.CommitIntervalSeconds))
		if err != nil {
			return nil, "kafka", errors.Wrap(err, "unable to parse CommitIntervalSeconds")
		}

		maxWait, err := time.ParseDuration(fmt.Sprintf("%ds", args.MaxWaitSeconds))
		if err != nil {
			return nil, "kafka", errors.Wrap(err, "unable to parse MaxWaitSeconds")
		}

		rebalanceTimeout, err := time.ParseDuration(fmt.Sprintf("%ds", args.RebalanceTimeoutSeconds))
		if err != nil {
			return nil, "kafka", errors.Wrap(err, "unable to parse RebalanceTimeoutSeconds")
		}

		connectTimeout, err := time.ParseDuration(fmt.Sprintf("%ds", cfg.TimeoutSeconds))
		if err != nil {
			return nil, "kafka", errors.Wrap(err, "unable to parse TimeoutSeconds")
		}

		// TODO: I think all relays should take their own unique struct instead of passing cli.Options
		opts := &cli.Options{
			Kafka: &cli.KafkaOptions{
				Brokers:            cfg.Address,
				Topics:             args.Topics,
				Timeout:            connectTimeout,
				InsecureTLS:        cfg.InsecureTls,
				Username:           cfg.SaslUsername,
				Password:           cfg.SaslPassword,
				AuthenticationType: cfg.SaslType.String(),
				UseConsumerGroup:   args.UseConsumerGroup,
				GroupID:            args.ConsumerGroupName,
				ReadOffset:         args.ReadOffset,
				MaxWait:            maxWait,
				MinBytes:           int(args.MinBytes),
				MaxBytes:           int(args.MaxBytes),
				QueueCapacity:      1, // TODO: protos?
				RebalanceTimeout:   rebalanceTimeout,
				CommitInterval:     commitInterval,
				WriteKey:           "",
				WriteHeader:        nil,
			},
		}
		rr, err = kafka.Relay(opts, relayCh, shutdownCtx)
		if err != nil {
			return rr, "kafka", err
		}
	default:
		return nil, "", errors.New("unknown relay type")
	}

	return rr, relayType, nil
}
