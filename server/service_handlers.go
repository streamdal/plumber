package server

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber/validate"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"

	"github.com/batchcorp/plumber/embed/etcd"
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
	_, err = s.Etcd.Put(ctx, etcd.CacheServicesPrefix+"/"+svc.Id, string(data))
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

	data, err := proto.Marshal(svc)
	if err != nil {
		return nil, errors.Wrap(err, "unable to save service to etcd")
	}

	// Save to etcd
	_, err = s.Etcd.Put(ctx, etcd.CacheServicesPrefix+"/"+svc.Id, string(data))
	if err != nil {
		return nil, errors.Wrapf(err, "unable to save service '%s' to etcd", svc.Id)
	}

	s.PersistentConfig.SetService(svc.Id, svc)

	if err := s.Etcd.PublishUpdateService(ctx, svc); err != nil {
		s.Log.Error(err)
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

func (s *Server) DeleteService(ctx context.Context, req *protos.DeleteServiceRequest) (*protos.DeleteServiceResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	svc := s.PersistentConfig.GetService(req.Id)
	if svc == nil {
		return nil, CustomError(common.Code_NOT_FOUND, validate.ErrServiceNotFound.Error())
	}

	// Delete in etcd
	_, err := s.Etcd.Delete(ctx, etcd.CacheServicesPrefix+"/"+svc.Id)
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
