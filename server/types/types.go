package types

import (
	"context"
	"fmt"
	"time"

	"github.com/batchcorp/plumber/backends/kafka"
	"github.com/batchcorp/plumber/cli"

	"github.com/pkg/errors"

	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber/relay"
)

type Relay struct {
	Active     bool
	Id         string
	ContextCxl context.Context
	CancelFunc context.CancelFunc
	RelayCh    chan interface{}
	Backend    relay.IRelayBackend
	Config     *protos.Relay
	log        *logrus.Entry
}

func (r *Relay) StartRelay(conn *protos.Connection) error {

	relayCh := make(chan interface{})

	// Needed to satisfy relay.Config{}, not used
	_, stubCancelFunc := context.WithCancel(context.Background())

	rr, relayType, err := getRelayBackend(r.Config, conn, relayCh, r.ContextCxl)
	if err != nil {
		return err
	}

	relayCfg := &relay.Config{
		Token:              r.Config.BatchCollectionToken,
		GRPCAddress:        r.Config.BatchshGrpcAddress,
		NumWorkers:         5,                // TODO: protos?
		Timeout:            time.Second * 10, // TODO: protos?
		RelayCh:            relayCh,
		DisableTLS:         r.Config.BatchshGrpcDisableTls,
		BatchSize:          int(r.Config.BatchSize),
		Type:               relayType,
		MainShutdownFunc:   stubCancelFunc,
		ServiceShutdownCtx: r.ContextCxl,
	}

	grpcRelayer, err := relay.New(relayCfg)
	if err != nil {
		return err
	}

	// Launch gRPC Workers
	if err := grpcRelayer.StartWorkers(r.ContextCxl); err != nil {
		return errors.Wrap(err, "unable to start gRPC relay workers")
	}

	// TODO: The relay needs to be ran in a goroutine so it continues in the background, but we
	// TODO: need to check if it errors on startup somehow. Maybe move reader initialization outside of kafka.Relay()?
	go func() {
		if err := rr.Relay(); err != nil {
			return
		}
	}()

	r.Backend = rr

	return nil
}

func getRelayBackend(
	req *protos.Relay,
	conn *protos.Connection,
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
