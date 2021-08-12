package server

import (
	"context"
	"fmt"

	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
)

func (p *PlumberServer) GetAllSchemas(_ context.Context, req *protos.GetAllSchemasRequest) (*protos.GetAllSchemasResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	schemas := make([]*protos.Schema, 0)

	for _, v := range p.PersistentConfig.Schemas {
		schemas = append(schemas, v.Schema)
	}

	return &protos.GetAllSchemasResponse{
		Schema: schemas,
	}, nil
}

func (p *PlumberServer) GetSchema(_ context.Context, req *protos.GetSchemaRequest) (*protos.GetSchemaResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	schema := p.getSchema(req.Id)
	if schema == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "schema does not exist")
	}

	return &protos.GetSchemaResponse{
		Schema: schema.Schema,
	}, nil
}

func (p *PlumberServer) ImportGithub(ctx context.Context, req *protos.ImportGithubRequest) (*protos.ImportGithubResponse, error) {
	schema, err := p.importGithub(ctx, req)
	if err != nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, err.Error())
	}

	return &protos.ImportGithubResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "schema imported successfully",
			RequestId: uuid.NewV4().String(),
		},
		Id: schema.Schema.Id,
	}, nil
}

func (p *PlumberServer) ImportLocal(_ context.Context, req *protos.ImportLocalRequest) (*protos.ImportLocalResponse, error) {
	schema, err := p.importLocal(req)
	if err != nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, err.Error())
	}

	return &protos.ImportLocalResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "schema imported successfully",
			RequestId: uuid.NewV4().String(),
		},
		Id: schema.Id,
	}, nil
}

func (p *PlumberServer) DeleteSchema(_ context.Context, req *protos.DeleteSchemaRequest) (*protos.DeleteSchemaResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	schema := p.getSchema(req.Id)
	if schema == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "schema does not exist")
	}

	p.SchemasMutex.Lock()
	delete(p.PersistentConfig.Schemas, req.Id)
	p.SchemasMutex.Unlock()

	p.PersistentConfig.Save()

	return &protos.DeleteSchemaResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "schema deleted",
			RequestId: uuid.NewV4().String(),
		},
	}, nil

}
