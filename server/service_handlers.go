package server

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/batchcorp/plumber/bus"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"

	"github.com/batchcorp/plumber/validate"
)

func (s *Server) GetService(_ context.Context, req *protos.GetServiceRequest) (*protos.GetServiceResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	svc := s.PersistentConfig.GetService(req.Id)
	if svc == nil {
		return nil, CustomError(common.Code_NOT_FOUND, validate.ErrServiceNotFound.Error())
	}

	return &protos.GetServiceResponse{
		Service: svc,
		Status: &common.Status{
			Code:      common.Code_OK,
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (s *Server) GetAllServices(_ context.Context, req *protos.GetAllServicesRequest) (*protos.GetAllServicesResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	s.PersistentConfig.ServicesMutex.RLock()
	defer s.PersistentConfig.ServicesMutex.RUnlock()

	services := make([]*protos.Service, 0)

	for _, svc := range s.PersistentConfig.Services {
		services = append(services, svc)
	}

	return &protos.GetAllServicesResponse{
		Services: services,
		Status: &common.Status{
			Code:      common.Code_OK,
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (s *Server) CreateService(ctx context.Context, req *protos.CreateServiceRequest) (*protos.CreateServiceResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	svc := req.Service

	if err := validate.ServiceForServer(svc); err != nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, fmt.Sprintf("unable to validate service options: %s", err))
	}

	if len(svc.GetUsedSchemas()) > 0 {
		// TODO: validate the existence of the schema IDs when that code is merged
	}

	svc.Id = uuid.NewV4().String()

	data, err := proto.Marshal(svc)
	if err != nil {
		return nil, errors.Wrap(err, "unable to save service to etcd")
	}

	// Save to etcd
	_, err = s.Etcd.Put(ctx, bus.CacheServicesPrefix+"/"+svc.Id, string(data))
	if err != nil {
		return nil, errors.Wrapf(err, "unable to save schema '%s' to etcd", svc.Id)
	}

	s.PersistentConfig.SetService(svc.Id, svc)

	if err := s.Etcd.PublishCreateService(ctx, svc); err != nil {
		s.Log.Error(err)
	}

	return &protos.CreateServiceResponse{
		Service: svc,
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Service created",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (s *Server) UpdateService(ctx context.Context, req *protos.UpdateServiceRequest) (*protos.UpdateServiceResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	svc := s.PersistentConfig.GetService(req.Service.Id)
	if svc == nil {
		return nil, CustomError(common.Code_NOT_FOUND, validate.ErrServiceNotFound.Error())
	}

	if err := validate.ServiceForServer(req.Service); err != nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, fmt.Sprintf("unable to validate service options: %s", err))
	}

	svc = req.Service

	if err := s.persistService(ctx, svc); err != nil {
		return nil, CustomError(common.Code_INTERNAL, err.Error())
	}

	return &protos.UpdateServiceResponse{
		Service: svc,
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Service updated",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

// persistService is a helper function to save updates to services
func (s *Server) persistService(ctx context.Context, svc *protos.Service) error {
	data, err := proto.Marshal(svc)
	if err != nil {
		return errors.Wrap(err, "unable to save service to etcd")
	}

	// Save to etcd
	_, err = s.Etcd.Put(ctx, bus.CacheServicesPrefix+"/"+svc.Id, string(data))
	if err != nil {
		return errors.Wrapf(err, "unable to save service '%s' to etcd", svc.Id)
	}

	s.PersistentConfig.SetService(svc.Id, svc)

	if err := s.Etcd.PublishUpdateService(ctx, svc); err != nil {
		s.Log.Error(err)
	}

	return nil
}

func (s *Server) DeleteService(ctx context.Context, req *protos.DeleteServiceRequest) (*protos.DeleteServiceResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	svc := s.PersistentConfig.GetService(req.Id)
	if svc == nil {
		return nil, CustomError(common.Code_NOT_FOUND, validate.ErrServiceNotFound.Error())
	}

	// Delete in etcd
	_, err := s.Etcd.Delete(ctx, bus.CacheServicesPrefix+"/"+svc.Id)
	if err != nil {
		return nil, CustomError(common.Code_INTERNAL, fmt.Sprintf("unable to delete service: "+err.Error()))
	}

	// Delete in memory
	s.PersistentConfig.DeleteService(svc.Id)

	// Publish DeleteService event
	if err := s.Etcd.PublishDeleteService(ctx, svc); err != nil {
		s.Log.Error(err)
	}

	return &protos.DeleteServiceResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Service deleted",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (s *Server) LinkSchemaToService(ctx context.Context, req *protos.LinkSchemaToServiceRequest) (*protos.LinkSchemaToServiceResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	svc := s.PersistentConfig.GetService(req.ServiceId)
	if svc == nil {
		return nil, CustomError(common.Code_NOT_FOUND, validate.ErrServiceNotFound.Error())
	}

	schema := s.PersistentConfig.GetSchema(req.SchemaId)
	if schema == nil {
		return nil, CustomError(common.Code_NOT_FOUND, validate.ErrSchemaNotFound.Error())
	}

	if serviceHasSchema(svc, schema) {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, "Service is already using this schema")
	}

	svc.UsedSchemas = append(svc.UsedSchemas, req.SchemaId)

	if err := s.persistService(ctx, svc); err != nil {
		return nil, CustomError(common.Code_INTERNAL, err.Error())
	}
	return &protos.LinkSchemaToServiceResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Schema linked to service",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}
func (s *Server) UnlinkSchemaFromService(ctx context.Context, req *protos.UnlinkSchemaFromServiceRequest) (*protos.UnlinkSchemaFromServiceResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	svc := s.PersistentConfig.GetService(req.ServiceId)
	if svc == nil {
		return nil, CustomError(common.Code_NOT_FOUND, validate.ErrServiceNotFound.Error())
	}

	schema := s.PersistentConfig.GetSchema(req.SchemaId)
	if schema == nil {
		return nil, CustomError(common.Code_NOT_FOUND, validate.ErrSchemaNotFound.Error())
	}

	removeSchemaFromService(svc, schema)

	if err := s.persistService(ctx, svc); err != nil {
		return nil, CustomError(common.Code_INTERNAL, err.Error())
	}

	return &protos.UnlinkSchemaFromServiceResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Schema unlinked from service",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func removeSchemaFromService(svc *protos.Service, schema *protos.Schema) {
	for i, s := range svc.GetUsedSchemas() {
		if s != schema.Id {
			continue
		}

		svc.UsedSchemas = append(svc.UsedSchemas[:i], svc.UsedSchemas[i+1:]...)

		return
	}
}

// serviceHasSchema checks if a service has the schema linked to it
func serviceHasSchema(svc *protos.Service, schema *protos.Schema) bool {
	for _, s := range svc.GetUsedSchemas() {
		if s == schema.Id {
			return true
		}
	}

	return false
}

func (s *Server) LinkRepoToService(ctx context.Context, req *protos.LinkRepoToServiceRequest) (*protos.LinkRepoToServiceResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	svc := s.PersistentConfig.GetService(req.ServiceId)
	if svc == nil {
		return nil, CustomError(common.Code_NOT_FOUND, validate.ErrServiceNotFound.Error())
	}

	repo, err := parseRepoURL(req.RepoUrl)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	svc.Repo = repo

	if err := s.persistService(ctx, svc); err != nil {
		return nil, CustomError(common.Code_INTERNAL, err.Error())
	}

	return &protos.LinkRepoToServiceResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Repo linked to service",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

// parseRepoURL takes a github/gitlab/bitbucket URL and parses it into a protos.Repository message
func parseRepoURL(repoURL string) (*protos.Repository, error) {
	parsedURL, err := url.Parse(repoURL)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse repository URL")
	}

	switch parsedURL.Host {
	case "github.com":
		fallthrough
	case "www.github.com":
		parts := strings.Split(strings.TrimPrefix(parsedURL.Path, "/"), "/")
		if len(parts) < 2 {
			return nil, fmt.Errorf("cannot parse URL '%s'", parsedURL.Path)
		}
		return &protos.Repository{
			XId:          uuid.NewV4().String(),
			Type:         protos.Repository_GITHUB,
			Organization: parts[0],
			Name:         parts[1],
		}, nil
	case "bitbucket.com":
		return nil, errors.New("bitbucket repos are not supported yet")
	case "gitlab.com":
		return nil, errors.New("gitlab repos are not supported yet")
	default:
		return nil, fmt.Errorf("cannot support '%s' repositories at this time", parsedURL.Host)
	}
}

func (s *Server) UnlinkRepoFromService(ctx context.Context, req *protos.UnlinkRepoFromServiceRequest) (*protos.UnlinkRepoFromServiceResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	svc := s.PersistentConfig.GetService(req.ServiceId)
	if svc == nil {
		return nil, CustomError(common.Code_NOT_FOUND, validate.ErrServiceNotFound.Error())
	}

	if svc.Repo == nil || svc.Repo.XId != req.RepoId {
		return nil, CustomError(common.Code_NOT_FOUND, validate.ErrRepoNotFound.Error())
	}

	svc.Repo = nil

	if err := s.persistService(ctx, svc); err != nil {
		return nil, CustomError(common.Code_INTERNAL, err.Error())
	}

	return &protos.UnlinkRepoFromServiceResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Repo unlinked from service",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}
