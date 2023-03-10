package server

import (
	"context"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
)

func (s *Server) GetChannel(_ context.Context, req *protos.GetChannelRequest) (*protos.GetChannelResponse, error) {
	return nil, nil
}

func (s *Server) GetAllChannels(_ context.Context, req *protos.GetAllChannelsRequest) (*protos.GetAllChannelsResponse, error) {
	return nil, nil
}

func (s *Server) CreateChannel(_ context.Context, req *protos.CreateChannelRequest) (*protos.CreateChannelResponse, error) {
	return nil, nil
}

func (s *Server) UpdateChannel(_ context.Context, req *protos.UpdateChannelRequest) (*protos.UpdateChannelResponse, error) {
	return nil, nil
}

func (s *Server) DeleteChannel(_ context.Context, req *protos.DeleteChannelRequest) (*protos.DeleteChannelResponse, error) {
	return nil, nil
}

func (s *Server) StopChannel(_ context.Context, req *protos.StopChannelRequest) (*protos.StopChannelResponse, error) {
	return nil, nil
}

func (s *Server) ResumeChannel(_ context.Context, req *protos.ResumeChannelRequest) (*protos.ResumeChannelResponse, error) {
	return nil, nil
}
