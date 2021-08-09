package types

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/jsonpb"

	"github.com/batchcorp/plumber/backends/kafka"
	"github.com/batchcorp/plumber/options"

	"github.com/pkg/errors"

	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber/relay"
)

// Connection is a wrapper around protos.Connect so that we can implement Marshaler interface
type Connection struct {
	*protos.Connection
}

type Relay struct {
	Active     bool                `json:"-"`
	Id         string              `json:"-"`
	CancelCtx  context.Context     `json:"-"`
	CancelFunc context.CancelFunc  `json:"-"`
	RelayCh    chan interface{}    `json:"-"`
	Backend    relay.IRelayBackend `json:"-"`
	Config     *protos.Relay       `json:"config"`
	log        *logrus.Entry       `json:"-"`
}

// MarshalJSON marshals a connection to JSON
func (c *Connection) MarshalJSON() ([]byte, error) {
	m := jsonpb.Marshaler{}

	buf := bytes.NewBuffer([]byte(``))

	if err := m.Marshal(buf, c.Connection); err != nil {
		return nil, errors.Wrap(err, "could not marshal protos.Connection")
	}
	return buf.Bytes(), nil
}

// UnmarshalJSON unmarshals JSON into a connection struct
func (c *Connection) UnmarshalJSON(v []byte) error {
	conn := &protos.Connection{}

	if err := jsonpb.Unmarshal(bytes.NewBuffer(v), conn); err != nil {
		return errors.Wrap(err, "unable to unmarshal stored connection")
	}

	c.Connection = conn

	return nil
}

// MarshalJSON marshals a relay proto message into JSON
func (r *Relay) MarshalJSON() ([]byte, error) {
	m := jsonpb.Marshaler{}

	buf := bytes.NewBuffer([]byte(``))

	if err := m.Marshal(buf, r.Config); err != nil {
		return nil, errors.Wrap(err, "could not marshal protos.Relay")
	}
	return buf.Bytes(), nil
}

// UnmarshalJSON unmarshals JSON into a relay proto message
func (r *Relay) UnmarshalJSON(v []byte) error {
	cfg := &protos.Relay{}

	if err := jsonpb.Unmarshal(bytes.NewBuffer(v), cfg); err != nil {
		return errors.Wrap(err, "unable to unmarshal stored relay")
	}

	r.Config = cfg

	return nil
}

// StartRelay starts a configured relay, it's workers, and the GRPC workers
func (r *Relay) StartRelay(conn *protos.Connection) error {

	relayCh := make(chan interface{})

	// Needed to satisfy relay.Config{}, not used
	_, stubCancelFunc := context.WithCancel(context.Background())

	rr, relayType, err := getRelayBackend(r.Config, conn, relayCh, r.CancelCtx)
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
		ServiceShutdownCtx: r.CancelCtx,
	}

	grpcRelayer, err := relay.New(relayCfg)
	if err != nil {
		return err
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
		opts := &cfg.Options{
			Kafka: &cfg.KafkaOptions{
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
