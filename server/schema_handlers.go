package server

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber/github"

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
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	repo, err := parseRepoURL(req.GithubUrl)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	tree, err := s.GithubService.GetRepoTree(ctx, s.PersistentConfig.GitHubToken, repo.Organization, repo.Name)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	req.XId = uuid.NewV4().String()

	s.PersistentConfig.SetImportRequest(req.XId, req)

	return &protos.ImportGithubResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "choose file",
			RequestId: uuid.NewV4().String(),
		},
		Id:   req.XId,
		Tree: github.TreeToDisplay(tree.Entries, req.Type),
	}, nil
}

func (s *Server) ImportGithubSelect(ctx context.Context, req *protos.ImportGithubSelectRequest) (*protos.ImportGithubSelectResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	importReq := s.PersistentConfig.GetImportRequest(req.ImportId)
	if importReq == nil {
		return nil, CustomError(common.Code_NOT_FOUND, fmt.Sprintf("could not find import request '%s'", req.ImportId))
	}

	schema, err := s.importGithub(ctx, importReq, req)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Save to memory
	s.PersistentConfig.SetSchema(schema.Id, schema)
	s.PersistentConfig.Save()

	// Publish create event
	if err := s.Bus.PublishCreateSchema(ctx, schema); err != nil {
		s.Log.Error(err)
	}

	return &protos.ImportGithubSelectResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Schema imported successfully",
			RequestId: uuid.NewV4().String(),
		},
		Schema: schema,
	}, nil
}

func (s *Server) ImportLocal(ctx context.Context, req *protos.ImportLocalRequest) (*protos.ImportLocalResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	schema, err := s.importLocal(req)
	if err != nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, err.Error())
	}

	// Save to memory
	s.PersistentConfig.SetSchema(schema.Id, schema)
	s.PersistentConfig.Save()

	// Publish create event
	if err := s.Bus.PublishCreateSchema(ctx, schema); err != nil {
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

func (s *Server) UpdateSchema(ctx context.Context, req *protos.UpdateSchemaRequest) (*protos.UpdateSchemaResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	schema := s.PersistentConfig.GetSchema(req.Id)
	if schema == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "schema does not exist")
	}

	schema.Name = req.Name
	schema.OwnerId = req.OwnerId
	schema.Notes = req.Notes

	// Save to memory
	s.PersistentConfig.SetSchema(schema.Id, schema)
	s.PersistentConfig.Save()

	// Publish create event
	if err := s.Bus.PublishUpdateSchema(ctx, schema); err != nil {
		s.Log.Error(err)
	}

	return &protos.UpdateSchemaResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "schema updated",
			RequestId: uuid.NewV4().String(),
		},
		Schema: schema,
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

	// Delete in memory
	s.PersistentConfig.DeleteConnection(schema.Id)
	s.PersistentConfig.Save()

	// Publish delete event
	if err := s.Bus.PublishDeleteSchema(ctx, schema); err != nil {
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

func (s *Server) DeleteSchemaVersion(ctx context.Context, req *protos.DeleteSchemaVersionRequest) (*protos.DeleteSchemaVersionResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	schema := s.PersistentConfig.GetSchema(req.Id)
	if schema == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "schema does not exist")
	}

	for i, v := range schema.Versions {
		if v.Version != req.Version {
			continue
		}

		schema.Versions = append(schema.Versions[:i], schema.Versions[i+1:]...)
		break
	}

	s.persistSchema(ctx, false, schema)

	return &protos.DeleteSchemaVersionResponse{
		Schema: schema,
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   fmt.Sprintf("Deleted version '%d' for schema '%s'", req.Version, req.Id),
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (s *Server) ApproveSchema(ctx context.Context, req *protos.ApproveSchemaVersionRequest) (*protos.ApproveSchemaVersionResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	schema := s.PersistentConfig.GetSchema(req.Id)
	if schema == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "schema does not exist")
	}

	for _, v := range schema.Versions {
		if v.Version != req.Version {
			continue
		}

		v.Status = protos.SchemaStatus_SCHEMA_STATUS_ACCEPTED
		break
	}

	s.persistSchema(ctx, false, schema)

	return &protos.ApproveSchemaVersionResponse{
		Schema: schema,
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   fmt.Sprintf("Deleted version '%d' for schema '%s'", req.Version, req.Id),
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (s *Server) GetRepoList(ctx context.Context, req *protos.GetRepoListRequest) (*protos.GetRepoListResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	repos, err := s.GithubService.GetRepoList(ctx, s.PersistentConfig.GitHubInstallID, s.PersistentConfig.GitHubToken)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	return &protos.GetRepoListResponse{
		RepositoryUrls: repos,
	}, nil
}
