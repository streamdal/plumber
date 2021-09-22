package server

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber/embed/etcd"

	"github.com/golang/protobuf/proto"

	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
)

func (s *Server) GetAllSchemas(_ context.Context, req *protos.GetAllSchemasRequest) (*protos.GetAllSchemasResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	schemas := make([]*protos.Schema, 0)

	for _, v := range s.PersistentConfig.Schemas {
		schemas = append(schemas, v)
	}

	return &protos.GetAllSchemasResponse{
		Schema: schemas,
	}, nil
}

func (s *Server) GetSchema(_ context.Context, req *protos.GetSchemaRequest) (*protos.GetSchemaResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	schema := s.PersistentConfig.GetSchema(req.Id)
	if schema == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "schema does not exist")
	}

	return &protos.GetSchemaResponse{
		Schema: schema,
	}, nil
}

func (s *Server) ImportGithub(ctx context.Context, req *protos.ImportGithubRequest) (*protos.ImportGithubResponse, error) {
	schema, err := s.importGithub(ctx, req)
	if err != nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, err.Error())
	}

	data, err := proto.Marshal(schema)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, "could not marshal schema")
	}

	// Save to etcd
	_, err = s.Etcd.Put(ctx, etcd.CacheSchemasPrefix+"/"+schema.Id, string(data))
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Save to memory
	s.PersistentConfig.SetSchema(schema.Id, schema)

	// Publish create event
	if err := s.Etcd.PublishCreateSchema(ctx, schema); err != nil {
		s.Log.Error(err)
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

func (s *Server) ImportLocal(ctx context.Context, req *protos.ImportLocalRequest) (*protos.ImportLocalResponse, error) {
	schema, err := s.importLocal(req)
	if err != nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, err.Error())
	}

	data, err := proto.Marshal(schema)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, "could not marshal schema")
	}

	// Save to etcd
	_, err = s.Etcd.Put(ctx, etcd.CacheSchemasPrefix+"/"+schema.Id, string(data))
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Save to memory
	s.PersistentConfig.SetSchema(schema.Id, schema)

	// Publish create event
	if err := s.Etcd.PublishCreateSchema(ctx, schema); err != nil {
		s.Log.Error(err)
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

func (s *Server) DeleteSchema(ctx context.Context, req *protos.DeleteSchemaRequest) (*protos.DeleteSchemaResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	schema := s.PersistentConfig.GetSchema(req.Id)
	if schema == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "schema does not exist")
	}

	// Delete in etcd
	_, err := s.Etcd.Delete(ctx, etcd.CacheSchemasPrefix+"/"+schema.Id)
	if err != nil {
		return nil, CustomError(common.Code_INTERNAL, fmt.Sprintf("unable to delete connection: "+err.Error()))
	}

	// Delete in memory
	s.PersistentConfig.DeleteConnection(schema.Id)

	// Publish delete event
	if err := s.Etcd.PublishDeleteSchema(ctx, schema); err != nil {
		s.Log.Error(err)
	}

	return &protos.DeleteSchemaResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "schema deleted",
			RequestId: uuid.NewV4().String(),
		},
	}, nil

}
