package server

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"

	"github.com/pkg/errors"

	"github.com/batchcorp/plumber/server/types"

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

// importGithub imports a github repo as a schema
// TODO: types other than protobuf
func (p *PlumberServer) importGithub(ctx context.Context, req *protos.ImportGithubRequest) (*types.Schema, error) {
	zipfile, err := p.GithubService.GetRepoArchive(ctx, p.PersistentConfig.GitHubToken, req.GithubUrl)
	if err != nil {
		return nil, err
	}

	var schema *types.Schema
	// Parse zip file
	if req.Type == encoding.Type_PROTOBUF {
		md, err := ProcessProtobufArchive(req.RootType, zipfile)
		if err != nil {
			return nil, errors.Wrap(err, "unable to parse protobuf zip")
		}

		schema = &types.Schema{
			Schema: &protos.Schema{
				Id:                uuid.NewV4().String(),
				Name:              req.Name,
				Type:              encoding.Type_PROTOBUF,
				Data:              zipfile,
				RootType:          req.RootType,
				MessageDescriptor: []byte(md.String()),
			},
		}
	} else if req.Type == encoding.Type_AVRO {
		// TODO
	} else {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, "only protobuf and avro schemas can be imported from github")
	}

	p.setSchema(schema.Schema.Id, schema)
	p.PersistentConfig.Save()

	return schema, nil
}

func (p *PlumberServer) ImportLocal(_ context.Context, req *protos.ImportLocalRequest) (*protos.ImportLocalResponse, error) {
	return &protos.ImportLocalResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "schema imported successfully",
			RequestId: uuid.NewV4().String(),
		},
		Id: "",
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

	return &protos.DeleteSchemaResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "schema deleted",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}
