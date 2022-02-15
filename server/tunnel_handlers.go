package server

import (
	"context"
	"fmt"

	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

func (s *Server) GetAllTunnels(_ context.Context, req *protos.GetAllTunnelsRequest) (*protos.GetAllTunnelsResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	var numActive int
	var numInactive int

	tunnelOptions := make([]*opts.TunnelOptions, 0)
	for _, v := range s.PersistentConfig.Tunnels {
		if v.Active {
			numActive += 1
		} else {
			numInactive += 1
		}

		tunnelOptions = append(tunnelOptions, v.Options)
	}

	msg := fmt.Sprintf("found '%d' active and '%d' inactive tunnels", numActive, numInactive)

	return &protos.GetAllTunnelsResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   msg,
			RequestId: uuid.NewV4().String(),
		},
		Opts: tunnelOptions,
	}, nil
}

func (s *Server) GetTunnel(_ context.Context, request *protos.GetTunnelRequest) (*protos.GetTunnelResponse, error) {
	if err := s.validateAuth(request.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	if request.TunnelId == "" {
		return nil, CustomError(common.Code_INVALID_ARGUMENT, "id cannot be empty")
	}

	tunnelCfg := s.PersistentConfig.GetTunnel(request.TunnelId)
	if tunnelCfg == nil {
		return nil, CustomError(common.Code_NOT_FOUND, fmt.Sprintf("tunnel not found: %s", request.TunnelId))
	}

	return &protos.GetTunnelResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			RequestId: uuid.NewV4().String(),
		},
		Opts: tunnelCfg.Options,
	}, nil
}

func (s *Server) CreateTunnel(ctx context.Context, req *protos.CreateTunnelRequest) (*protos.CreateTunnelResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	req.Opts.XTunnelId = uuid.NewV4().String()
	req.Opts.XActive = true

	d, err := s.Actions.CreateTunnel(ctx, req.Opts)
	if err != nil {
		s.rollbackCreateTunnel(ctx, req.Opts)
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Publish CreateTunnel event
	if err := s.Bus.PublishCreateTunnel(ctx, d.Options); err != nil {
		s.rollbackCreateTunnel(ctx, req.Opts)
		s.Log.Error(err)
	}

	s.Log.Infof("Tunnel '%s' created", d.Id)

	return &protos.CreateTunnelResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Tunnel tunnel created",
			RequestId: uuid.NewV4().String(),
		},
		TunnelId: d.Id,
	}, nil
}

func (s *Server) rollbackCreateTunnel(ctx context.Context, req *opts.TunnelOptions) {
	s.PersistentConfig.DeleteTunnel(req.XTunnelId)
	s.PersistentConfig.Save()
}

func (s *Server) UpdateTunnel(ctx context.Context, req *protos.UpdateTunnelRequest) (*protos.UpdateTunnelResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	if _, err := s.Actions.UpdateTunnel(ctx, req.TunnelId, req.Opts); err != nil {
		// No need to roll back here since we haven't updated anything yet
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	s.Log.Infof("Tunnel '%s' updated", req.TunnelId)

	return &protos.UpdateTunnelResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Relay updated",
			RequestId: uuid.NewV4().String(),
		},
	}, nil

}

func (s *Server) StopTunnel(ctx context.Context, req *protos.StopTunnelRequest) (*protos.StopTunnelResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	tunnelOptions, err := s.Actions.StopTunnel(ctx, req.TunnelId)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Publish CreateTunnel event
	if err := s.Bus.PublishStopTunnel(ctx, tunnelOptions.Options); err != nil {
		// TODO: Should have rollback
		s.Log.Errorf("unable to publish stop tunnel event: %s", err)
	}

	s.Log.Infof("Tunnel '%s' stopped", req.TunnelId)

	return &protos.StopTunnelResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Tunnel replay stopped",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (s *Server) ResumeTunnel(ctx context.Context, req *protos.ResumeTunnelRequest) (*protos.ResumeTunnelResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	d, err := s.Actions.ResumeTunnel(ctx, req.TunnelId)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Publish CreateTunnel event
	if err := s.Bus.PublishResumeTunnel(ctx, d.Options); err != nil {
		// TODO: Should have rollback
		s.Log.Errorf("unable to publish resume tunnel event: %s", err)
	}

	s.Log.Infof("Tunnel '%s' started", d.Id)

	return &protos.ResumeTunnelResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Tunnel replay resumed",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (s *Server) DeleteTunnel(ctx context.Context, req *protos.DeleteTunnelRequest) (*protos.DeleteTunnelResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	// Needed for PublishDeleteTunnel() below
	tunnelCfg := s.PersistentConfig.GetTunnel(req.TunnelId)
	if tunnelCfg == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "tunnel does not exist")
	}

	if err := s.Actions.DeleteTunnel(ctx, req.TunnelId); err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Publish delete event
	if err := s.Bus.PublishDeleteTunnel(ctx, tunnelCfg.Options); err != nil {
		s.Log.Error(err)
	}

	s.Log.Infof("Tunnel '%s' deleted", req.TunnelId)

	return &protos.DeleteTunnelResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Tunnel replay deleted",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}
