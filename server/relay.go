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
	Id         string
	ContextCxl context.Context
	CancelFunc context.CancelFunc
	RelayCh    chan interface{}
	log        *logrus.Entry
}

func (p *PlumberServer) CreateRelay(ctx context.Context, req *protos.CreateRelayRequest) (*protos.CreateRelayResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	// Get stored connection information
	conn := p.getConn(req.Relay.ConnectionId)
	if conn == nil {
		return &protos.CreateRelayResponse{
			Status: &common.Status{
				Code:      common.Code_NOT_FOUND,
				Message:   fmt.Sprintf("Connection '%s' does not exist", req.Relay.ConnectionId),
				RequestId: uuid.NewV4().String(),
			},
		}, nil
	}

	relayCh := make(chan interface{})

	// Needed to satisfy relay.Config{}, not used
	_, stubCancelFunc := context.WithCancel(context.Background())

	// Used to shutdown relays on StopRelay() gRPC call
	shutdownCtx, shutdownFunc := context.WithCancel(context.Background())

	rr, relayType, err := p.getRelayBackend(req, conn, relayCh, shutdownCtx)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	r := &Relay{
		Id:         uuid.NewV4().String(),
		ContextCxl: shutdownCtx,
		CancelFunc: shutdownFunc,
		log:        nil,
	}

	relayCfg := &relay.Config{
		Token:              req.Relay.BatchCollectionToken,
		GRPCAddress:        req.Relay.BatchshGrpcAddress,
		NumWorkers:         5,                // TODO: protos?
		Timeout:            time.Second * 10, // TODO: protos?
		RelayCh:            relayCh,
		DisableTLS:         req.Relay.BatchshGrpcDisableTls,
		BatchSize:          int(req.Relay.BatchSize),
		Type:               relayType,
		MainShutdownFunc:   stubCancelFunc,
		ServiceShutdownCtx: shutdownCtx,
	}

	grpcRelayer, err := relay.New(relayCfg)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Launch gRPC Relayer
	if err := grpcRelayer.StartWorkers(shutdownCtx); err != nil {
		return nil, errors.Wrap(err, "unable to start gRPC relay workers")
	}

	// TODO: The relay needs to be ran in a goroutine so it continues in the background, but we
	// TODO: need to check if it errors on startup somehow. Maybe move reader initialization outside of kafka.Relay()?
	go func() {
		if err := rr.Relay(); err != nil {
			return
		}
	}()

	p.setRelay(r.Id, r)

	return &protos.CreateRelayResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Relay started",
			RequestId: uuid.NewV4().String(),
		},
		RelayId: r.Id,
	}, nil
}

func (p *PlumberServer) UpdateRelay(ctx context.Context, req *protos.UpdateRelayRequest) (*protos.UpdateRelayResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	// TODO:

	return nil, nil
}

func (p *PlumberServer) StopRelay(ctx context.Context, req *protos.StopRelayRequest) (*protos.StopRelayResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	relay := p.getRelay(req.RelayId)
	if relay == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "relay does not exist")
	}

	// Stop workers
	relay.CancelFunc()

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

	// TODO

	return nil, nil
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

func (p *PlumberServer) getRelayBackend(
	req *protos.CreateRelayRequest,
	conn *protos.Connection,
	relayCh chan interface{},
	shutdownCtx context.Context,
) (rr relay.IRelayBackend, relayType string, err error) {

	switch {
	case req.GetKafka() != nil:
		args := req.GetKafka()
		cfg := conn.GetKafka()
		relayType = "kafka"

		commitInterval, _ := time.ParseDuration(fmt.Sprintf("%ds", args.CommitIntervalSeconds))
		maxWait, _ := time.ParseDuration(fmt.Sprintf("%ds", args.MaxWaitSeconds))
		rebalanceTimeout, _ := time.ParseDuration(fmt.Sprintf("%ds", args.RebalanceTimeoutSeconds))
		connectTimeout, _ := time.ParseDuration(fmt.Sprintf("%ds", cfg.TimeoutSeconds))

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
