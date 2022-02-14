package server

import (
	"context"
	"fmt"

	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

func (s *Server) GetAllDynamic(_ context.Context, req *protos.GetAllDynamicRequest) (*protos.GetAllDynamicResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	var numActive int
	var numInactive int

	dynamicOptions := make([]*opts.DynamicOptions, 0)
	for _, v := range s.PersistentConfig.Dynamic {
		if v.Active {
			numActive += 1
		} else {
			numInactive += 1
		}

		dynamicOptions = append(dynamicOptions, v.Options)
	}

	msg := fmt.Sprintf("found '%d' active and '%d' inactive dynamic replays", numActive, numInactive)

	return &protos.GetAllDynamicResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   msg,
			RequestId: uuid.NewV4().String(),
		},
		Opts: dynamicOptions,
	}, nil
}

func (s *Server) GetDynamic(_ context.Context, request *protos.GetDynamicRequest) (*protos.GetDynamicResponse, error) {
	if err := s.validateAuth(request.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	if request.DynamicId == "" {
		return nil, CustomError(common.Code_INVALID_ARGUMENT, "id cannot be empty")
	}

	dynamic := s.PersistentConfig.GetDynamic(request.DynamicId)
	if dynamic == nil {
		return nil, CustomError(common.Code_NOT_FOUND, fmt.Sprintf("dynamic replay not found: %s", request.DynamicId))
	}

	return &protos.GetDynamicResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			RequestId: uuid.NewV4().String(),
		},
		Opts: dynamic.Options,
	}, nil
}

func (s *Server) CreateDynamic(ctx context.Context, req *protos.CreateDynamicRequest) (*protos.CreateDynamicResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	req.Opts.XDynamicId = uuid.NewV4().String()
	req.Opts.XActive = true

	d, err := s.Actions.CreateDynamic(ctx, req.Opts)
	if err != nil {
		s.rollbackCreateDynamic(ctx, req.Opts)
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Publish CreateDynamic event
	if err := s.Bus.PublishCreateDynamic(ctx, d.Options); err != nil {
		s.rollbackCreateDynamic(ctx, req.Opts)
		s.Log.Error(err)
	}

	s.Log.Infof("Replay tunnel '%s' started", d.Id)

	return &protos.CreateDynamicResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Dynamic tunnel created",
			RequestId: uuid.NewV4().String(),
		},
		DynamicId: d.Id,
	}, nil
}

func (s *Server) rollbackCreateDynamic(ctx context.Context, req *opts.DynamicOptions) {
	s.PersistentConfig.DeleteDynamic(req.XDynamicId)
	s.PersistentConfig.Save()
}

func (s *Server) UpdateDynamic(ctx context.Context, req *protos.UpdateDynamicRequest) (*protos.UpdateDynamicResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	if _, err := s.Actions.UpdateDynamic(ctx, req.DynamicId, req.Opts); err != nil {
		// No need to roll back here since we haven't updated anything yet
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	return &protos.UpdateDynamicResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Relay updated",
			RequestId: uuid.NewV4().String(),
		},
	}, nil

}

func (s *Server) StopDynamic(ctx context.Context, req *protos.StopDynamicRequest) (*protos.StopDynamicResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	dynamicOptions, err := s.Actions.StopDynamic(ctx, req.DynamicId)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Publish CreateDynamic event
	if err := s.Bus.PublishStopDynamic(ctx, dynamicOptions.Options); err != nil {
		// TODO: Should have rollback
		s.Log.Errorf("unable to publish stop dynamic event: %s", err)
	}

	s.Log.Infof("Dynamic replay '%s' stopped", req.DynamicId)

	return &protos.StopDynamicResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Dynamic replay stopped",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (s *Server) ResumeDynamic(ctx context.Context, req *protos.ResumeDynamicRequest) (*protos.ResumeDynamicResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	d, err := s.Actions.ResumeDynamic(ctx, req.DynamicId)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Publish CreateDynamic event
	if err := s.Bus.PublishResumeDynamic(ctx, d.Options); err != nil {
		// TODO: Should have rollback
		s.Log.Errorf("unable to publish resume dynamic event: %s", err)
	}

	s.Log.Infof("Dynamic replay '%s' started", d.Id)

	return &protos.ResumeDynamicResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Dynamic replay resumed",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (s *Server) DeleteDynamic(ctx context.Context, req *protos.DeleteDynamicRequest) (*protos.DeleteDynamicResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	// Needed for PublishDeleteDynamic() below
	dynamicReplay := s.PersistentConfig.GetDynamic(req.DynamicId)
	if dynamicReplay == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "dynamic replay does not exist")
	}

	if err := s.Actions.DeleteDynamic(ctx, req.DynamicId); err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Publish delete event
	if err := s.Bus.PublishDeleteDynamic(ctx, dynamicReplay.Options); err != nil {
		s.Log.Error(err)
	}

	s.Log.Infof("Dynamic replay '%s' deleted", req.DynamicId)

	return &protos.DeleteDynamicResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Dynamic replay deleted",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}
