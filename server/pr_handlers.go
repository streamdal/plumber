package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
)

func (s *Server) GetRepoTree(ctx context.Context, req *protos.GetRepoTreeRequest) (*protos.GetRepoTreeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetRepoTree not implemented")
}
func (s *Server) GetRepoFile(ctx context.Context, req *protos.GetRepoFileRequest) (*protos.GetRepoFileResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetRepoFile not implemented")
}
func (s *Server) CreatePullRequest(ctx context.Context, req *protos.CreatePRRequest) (*protos.CreatePRResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreatePullRequest not implemented")
}
