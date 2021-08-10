package server

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber/server/types"

	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
)

func (p *PlumberServer) GetService(_ context.Context, req *protos.GetServiceRequest) (*protos.GetServiceResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	svc := p.getService(req.Id)
	if svc == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "service does not exist")
	}

	return &protos.GetServiceResponse{
		Service: svc.Service,
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

	p.ServicesMutex.RLock()
	defer p.ServicesMutex.RUnlock()

	services := make([]*protos.Service, 0)

	for _, svc := range p.PersistentConfig.Services {
		services = append(services, svc.Service)
	}

	return &protos.GetAllServicesResponse{
		Services: services,
		Status: &common.Status{
			Code:      common.Code_OK,
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (p *PlumberServer) CreateService(_ context.Context, req *protos.CreateServiceRequest) (*protos.CreateServiceResponse, error) {
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

	p.setService(svc.Id, &types.Service{Service: svc})
	p.PersistentConfig.Save()

	return &protos.CreateServiceResponse{
		Service: svc,
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Service created",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (p *PlumberServer) UpdateService(_ context.Context, req *protos.UpdateServiceRequest) (*protos.UpdateServiceResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	svc := p.getService(req.Service.Id)
	if svc == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "service does not exist")
	}

	if err := validateService(req.Service); err != nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, err.Error())
	}

	svc.Service = req.Service

	p.setService(svc.Id, svc)
	p.PersistentConfig.Save()

	return &protos.UpdateServiceResponse{
		Service: svc.Service,
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Service updated",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (p *PlumberServer) DeleteService(_ context.Context, req *protos.DeleteServiceRequest) (*protos.DeleteServiceResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	if _, ok := p.PersistentConfig.Services[req.Id]; !ok {
		return nil, CustomError(common.Code_NOT_FOUND, "service does not exist")
	}

	p.ServicesMutex.Lock()
	delete(p.PersistentConfig.Services, req.Id)
	p.ServicesMutex.Unlock()

	p.PersistentConfig.Save()

	return &protos.DeleteServiceResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Service deleted",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}
