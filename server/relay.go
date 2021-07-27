package server

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber/server/types"
)

func (p *PlumberServer) GetAllRelays(_ context.Context, req *protos.GetAllRelaysRequest) (*protos.GetAllRelaysResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	relays := make([]*protos.Relay, 0)
	for _, v := range p.PersistentConfig.Relays {
		relays = append(relays, v.Config)
	}

	return &protos.GetAllRelaysResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			RequestId: uuid.NewV4().String(),
		},
		Relays: relays,
	}, nil
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

	r := &types.Relay{
		Id:         uuid.NewV4().String(),
		CancelFunc: shutdownFunc,
		ContextCxl: shutdownCtx,
		Config:     req.Relay,
	}

	if err := r.StartRelay(conn); err != nil {
		return nil, errors.Wrap(err, "unable to start relay")
	}

	r.Config.RelayId = r.Id
	r.Active = true
	p.setRelay(r.Id, r)

	p.Log.Infof("Relay '%s' started", r.Id)

	if err := p.PersistentConfig.Save(); err != nil {
		p.Log.Errorf("Could not persist config to disk: %s", err)
	}

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

	relay.Active = true

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
	relay.Active = false

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

	relay.Active = true

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
	delete(p.PersistentConfig.Relays, req.RelayId)

	if err := p.PersistentConfig.Save(); err != nil {
		p.Log.Errorf("Could not persist config to disk: %s", err)
	}

	return &protos.DeleteRelayResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Relay deleted",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}
