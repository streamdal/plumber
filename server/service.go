package server

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
)

func (p *PlumberServer) GetService(_ context.Context, req *protos.GetServiceRequest) (*protos.GetServiceResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	svc := p.PersistentConfig.GetService(req.Id)
	if svc == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "service does not exist")
	}

	return &protos.GetServiceResponse{
		Service: svc,
		Status: &common.Status{
			Code:      common.Code_OK,
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (p *PlumberServer) GetAllServices(_ context.Context, req *protos.GetAllServicesRequest) (*protos.GetAllServicesResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	p.PersistentConfig.ServicesMutex.RLock()
	defer p.PersistentConfig.ServicesMutex.RUnlock()

	services := make([]*protos.Service, 0)

	for _, svc := range p.PersistentConfig.Services {
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

func (p *PlumberServer) CreateService(ctx context.Context, req *protos.CreateServiceRequest) (*protos.CreateServiceResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	svc := req.Service

	if err := validateService(svc); err != nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, err.Error())
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
	_, err = p.Etcd.Put(ctx, EtcdServicesPrefix+"/"+svc.Id, string(data))
	if err != nil {
		return nil, errors.Wrapf(err, "unable to save schema '%s' to etcd", svc.Id)
	}

	p.PersistentConfig.SetService(svc.Id, svc)

	return &protos.CreateServiceResponse{
		Service: svc,
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Service created",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (p *PlumberServer) UpdateService(ctx context.Context, req *protos.UpdateServiceRequest) (*protos.UpdateServiceResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	svc := p.PersistentConfig.GetService(req.Service.Id)
	if svc == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "service does not exist")
	}

	if err := validateService(req.Service); err != nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, err.Error())
	}

	svc = req.Service

	data, err := proto.Marshal(svc)
	if err != nil {
		return nil, errors.Wrap(err, "unable to save service to etcd")
	}

	// Save to etcd
	_, err = p.Etcd.Put(ctx, EtcdServicesPrefix+"/"+svc.Id, string(data))
	if err != nil {
		return nil, errors.Wrapf(err, "unable to save service '%s' to etcd", svc.Id)
	}

	p.PersistentConfig.SetService(svc.Id, svc)

	if err := p.Etcd.PublishUpdateService(ctx, svc); err != nil {
		p.Log.Error(err)
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

func (p *PlumberServer) DeleteService(ctx context.Context, req *protos.DeleteServiceRequest) (*protos.DeleteServiceResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	svc := p.PersistentConfig.GetService(req.Id)
	if svc == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "service does not exist")
	}

	// Delete in etcd
	_, err := p.Etcd.Delete(ctx, EtcdServicesPrefix+"/"+svc.Id)
	if err != nil {
		return nil, CustomError(common.Code_INTERNAL, fmt.Sprintf("unable to delete service: "+err.Error()))
	}

	// Delete in memory
	p.PersistentConfig.DeleteService(svc.Id)

	// Publish DeleteService event
	if err := p.Etcd.PublishDeleteService(ctx, svc); err != nil {
		p.Log.Error(err)
	}

	return &protos.DeleteServiceResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Service deleted",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}
