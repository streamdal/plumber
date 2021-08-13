package server

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"

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
		schemas = append(schemas, v)
	}

	return &protos.GetAllSchemasResponse{
		Schema: schemas,
	}, nil
}

func (p *PlumberServer) GetSchema(_ context.Context, req *protos.GetSchemaRequest) (*protos.GetSchemaResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	schema := p.PersistentConfig.GetSchema(req.Id)
	if schema == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "schema does not exist")
	}

	return &protos.GetSchemaResponse{
		Schema: schema,
	}, nil
}

func (p *PlumberServer) ImportGithub(ctx context.Context, req *protos.ImportGithubRequest) (*protos.ImportGithubResponse, error) {
	schema, err := p.importGithub(ctx, req)
	if err != nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, err.Error())
	}

	data, err := proto.Marshal(schema)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, "could not marshal connection")
	}

	// Save to etcd
	_, err = p.Etcd.Put(ctx, EtcdSchemasPrefix+"/"+schema.Id, string(data))
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Save to memory
	p.PersistentConfig.SetSchema(schema.Id, schema)

	// Publish create event
	if err := p.Etcd.PublishCreateSchema(ctx, schema); err != nil {
		p.Log.Error(err)
	}

	return &protos.ImportGithubResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "schema imported successfully",
			RequestId: uuid.NewV4().String(),
		},
		Id: schema.Id,
	}, nil
}

func (p *PlumberServer) ImportLocal(ctx context.Context, req *protos.ImportLocalRequest) (*protos.ImportLocalResponse, error) {
	schema, err := p.importLocal(req)
	if err != nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, err.Error())
	}

	data, err := proto.Marshal(schema)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, "could not marshal connection")
	}

	// Save to etcd
	_, err = p.Etcd.Put(ctx, EtcdSchemasPrefix+"/"+schema.Id, string(data))
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Save to memory
	p.PersistentConfig.SetSchema(schema.Id, schema)

	// Publish create event
	if err := p.Etcd.PublishCreateSchema(ctx, schema); err != nil {
		p.Log.Error(err)
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

func (p *PlumberServer) DeleteSchema(ctx context.Context, req *protos.DeleteSchemaRequest) (*protos.DeleteSchemaResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	schema := p.PersistentConfig.GetSchema(req.Id)
	if schema == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "schema does not exist")
	}

	// Delete in etcd
	_, err := p.Etcd.Delete(ctx, EtcdSchemasPrefix+"/"+schema.Id)
	if err != nil {
		return nil, CustomError(common.Code_INTERNAL, fmt.Sprintf("unable to delete connection: "+err.Error()))
	}

	// Delete in memory
	p.PersistentConfig.DeleteConnection(schema.Id)

	// Publish delete event
	if err := p.Etcd.PublishDeleteSchema(ctx, schema); err != nil {
		p.Log.Error(err)
	}

	return &protos.DeleteSchemaResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "schema deleted",
			RequestId: uuid.NewV4().String(),
		},
	}, nil

}
