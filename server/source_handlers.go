package server

import (
	"context"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
)

func (s *Server) GetSource(_ context.Context, req *protos.GetSourceRequest) (*protos.GetSourceResponse, error) {
	return nil, nil
}
func (s *Server) GetAllSources(_ context.Context, req *protos.GetAllSourcesRequest) (*protos.GetAllSourcesResponse, error) {
	return nil, nil
}
func (s *Server) CreateSource(_ context.Context, req *protos.CreateSourceRequest) (*protos.CreateSourceResponse, error) {
	return nil, nil
}
func (s *Server) UpdateSource(_ context.Context, req *protos.UpdateSourceRequest) (*protos.UpdateSourceResponse, error) {
	return nil, nil
}
func (s *Server) DeleteSource(_ context.Context, req *protos.DeleteSourceRequest) (*protos.DeleteSourceResponse, error) {
	return nil, nil
}
