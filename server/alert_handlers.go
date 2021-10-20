package server

import (
	"context"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
)

func (s *Server) GetAlert(ctx context.Context, req *protos.GetMonitorRequest) (*protos.GetMonitorResponse, error) {
	return nil, nil
}

func (s *Server) CreateAlert(ctx context.Context, req *protos.CreateAlertRequest) (*protos.CreateAlertResponse, error) {
	return nil, nil
}

func (s *Server) DeleteAlert(ctx context.Context, req *protos.DeleteAlertRequest) (*protos.DeleteAlertResponse, error) {
	return nil, nil
}

func (s *Server) UpdateAlert(ctx context.Context, req *protos.UpdateAlertRequest) (*protos.UpdateAlertResponse, error) {
	return nil, nil
}
