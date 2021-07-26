package server

import (
	"context"
	"fmt"
	"time"

	"github.com/batchcorp/plumber/backends/kafka"
	"github.com/batchcorp/plumber/cli"

	uuid "github.com/satori/go.uuid"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/relay"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
)

type Relay struct {
	Id         string              `json:"id"`
	ContextCxl context.Context     `json:"-"`
	CancelFunc context.CancelFunc  `json:"-"`
	RelayCh    chan interface{}    `json:"-"`
	Backend    relay.IRelayBackend `json:"-"`
	Config     *protos.Relay       `json:"config"`
	log        *logrus.Entry       `json:"-"`
}

func (r *Relay) StartRelay(conn *protos.Connection) error {

	relayCh := make(chan interface{})

	// Needed to satisfy relay.Config{}, not used
	_, stubCancelFunc := context.WithCancel(context.Background())

	rr, relayType, err := getRelayBackend(r.Config, conn, relayCh, r.ContextCxl)
	if err != nil {
		return CustomError(common.Code_ABORTED, err.Error())
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
		return CustomError(common.Code_ABORTED, err.Error())
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

func (p *PlumberServer) CreateRelay(_ context.Context, req *protos.CreateRelayRequest) (*protos.CreateRelayResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	// Get stored connection information
	conn := p.getConn(req.Relay.ConnectionId)
	if conn == nil {
		return nil, fmt.Errorf("connection '%s' does not exist", req.Relay.ConnectionId)
	}

	// Used to shutdown relays on StopRelay() gRPC call
	shutdownCtx, shutdownFunc := context.WithCancel(context.Background())

	r := &Relay{
		Id:         uuid.NewV4().String(),
		CancelFunc: shutdownFunc,
		ContextCxl: shutdownCtx,
		Config:     req.Relay,
	}

	if err := r.StartRelay(conn); err != nil {
		return nil, errors.Wrap(err, "unable to start relay")
	}

	p.setRelay(r.Id, r)

	p.Log.Infof("Relay '%s' started", r.Id)

	return &protos.CreateRelayResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Relay started",
			RequestId: uuid.NewV4().String(),
		},
		RelayId: r.Id,
	}, nil
}

func (p *PlumberServer) UpdateRelay(_ context.Context, req *protos.UpdateRelayRequest) (*protos.UpdateRelayResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	// Stop existing relay
	relay := p.getRelay(req.RelayId)
	if relay == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "relay does not exist")
	}

	// Stop workers
	relay.CancelFunc()

	p.Log.Infof("Relay '%s' stopped", relay.Id)

	// TODO: need to get signal of when relay shutdown is complete
	time.Sleep(time.Second)

	// Update relay config
	relay.Config = req.Relay

	// Restart relay
	conn := p.getConn(relay.Config.ConnectionId)
	if conn == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "connection does not exist")
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	relay.ContextCxl = ctx
	relay.CancelFunc = cancelFunc

	if err := relay.StartRelay(conn); err != nil {
		return nil, errors.Wrap(err, "unable to start relay")
	}

	p.Log.Infof("Relay '%s' started", relay.Id)

	return &protos.UpdateRelayResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Relay updated",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (p *PlumberServer) StopRelay(_ context.Context, req *protos.StopRelayRequest) (*protos.StopRelayResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	relay := p.getRelay(req.RelayId)
	if relay == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "relay does not exist")
	}

	// Stop workers
	relay.CancelFunc()

	p.Log.Infof("Relay '%s' stopped", relay.Id)

	return &protos.StopRelayResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Relay stopped",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (p *PlumberServer) ResumeRelay(ctx context.Context, req *protos.ResumeRelayRequest) (*protos.ResumeRelayResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	relay := p.getRelay(req.RelayId)
	if relay == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "relay does not exist")
	}

	conn := p.getConn(relay.Config.ConnectionId)
	if conn == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "connection does not exist")
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	relay.ContextCxl = ctx
	relay.CancelFunc = cancelFunc

	if err := relay.StartRelay(conn); err != nil {
		return nil, errors.Wrap(err, "unable to start relay")
	}

	p.Log.Infof("Relay '%s' started", relay.Id)

	return &protos.ResumeRelayResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Relay resumed",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (p *PlumberServer) DeleteRelay(_ context.Context, req *protos.DeleteRelayRequest) (*protos.DeleteRelayResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	relay := p.getRelay(req.RelayId)
	if relay == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "relay does not exist")
	}

	// Stop workers
	relay.CancelFunc()

	p.RelaysMutex.Lock()
	defer p.RelaysMutex.Unlock()
	delete(p.Relays, req.RelayId)

	return &protos.DeleteRelayResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Relay deleted",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
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
