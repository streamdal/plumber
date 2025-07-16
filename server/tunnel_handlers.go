package server

import (
	"context"
	"errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
)

// TODO: remove these handlers when protos are updated to remove tunnel opts,
// TODO: they need to be stubbged currently to satisfy the existing interface
func (s *Server) GetAllTunnels(_ context.Context, req *protos.GetAllTunnelsRequest) (*protos.GetAllTunnelsResponse, error) {
	return nil, errors.New("tunnels are no longer supported")
}

func (s *Server) GetTunnel(_ context.Context, request *protos.GetTunnelRequest) (*protos.GetTunnelResponse, error) {
	return nil, errors.New("tunnels are no longer supported")
}

func (s *Server) CreateTunnel(ctx context.Context, req *protos.CreateTunnelRequest) (*protos.CreateTunnelResponse, error) {
	return nil, errors.New("tunnels are no longer supported")
}

func (s *Server) UpdateTunnel(ctx context.Context, req *protos.UpdateTunnelRequest) (*protos.UpdateTunnelResponse, error) {
	return nil, errors.New("tunnels are no longer supported")
}

func (s *Server) StopTunnel(ctx context.Context, req *protos.StopTunnelRequest) (*protos.StopTunnelResponse, error) {
	return nil, errors.New("tunnels are no longer supported")
}

func (s *Server) ResumeTunnel(ctx context.Context, req *protos.ResumeTunnelRequest) (*protos.ResumeTunnelResponse, error) {
	return nil, errors.New("tunnels are no longer supported")
}

func (s *Server) DeleteTunnel(ctx context.Context, req *protos.DeleteTunnelRequest) (*protos.DeleteTunnelResponse, error) {
	return nil, errors.New("tunnels are no longer supported")
}
