package server

import (
	"context"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
)

func (p *PlumberServer) GetAllSchemas(_ context.Context, req *protos.GetAllSchemasRequest) (*protos.GetAllSchemasResponse, error) {
	return nil, nil
}

func (p *PlumberServer) GetSchema(_ context.Context, req *protos.GetSchemaRequest) (*protos.GetSchemaResponse, error) {
	return nil, nil
}

func (p *PlumberServer) ImportGithub(ctx context.Context, req *protos.ImportGithubRequest) (*protos.ImportGithubResponse, error) {
	return nil, nil
}

func (p *PlumberServer) ImportLocal(_ context.Context, req *protos.ImportLocalRequest) (*protos.ImportLocalResponse, error) {
	return nil, nil
}

func (p *PlumberServer) DeleteSchema(_ context.Context, req *protos.DeleteSchemaRequest) (*protos.DeleteSchemaResponse, error) {
	return nil, nil
}
