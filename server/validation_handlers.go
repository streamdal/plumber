package server

import (
	"context"
	"fmt"

	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
)

func (s *Server) GetValidation(_ context.Context, req *protos.GetValidationRequest) (*protos.GetValidationResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	validation := s.PersistentConfig.GetValidation(req.Id)
	if validation == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "validation not found")
	}

	return &protos.GetValidationResponse{
		Validation: validation,
	}, nil
}

func (s *Server) GetAllValidations(_ context.Context, req *protos.GetAllValidationsRequest) (*protos.GetAllValidationsResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	validations := make([]*common.Validation, 0)
	for _, v := range s.PersistentConfig.Validations {
		validations = append(validations, v)
	}

	return &protos.GetAllValidationsResponse{
		Validations: validations,
	}, nil
}

func (s *Server) CreateValidation(ctx context.Context, req *protos.CreateValidationRequest) (*protos.CreateValidationResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	requestID := uuid.NewV4().String()

	req.Validation.XId = uuid.NewV4().String()

	if err := s.persistValidation(ctx, req.Validation); err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Publish CreateValidation event for other plumber instances to receive
	if err := s.Bus.PublishCreateValidation(ctx, req.Validation); err != nil {
		s.Log.Error(err)
	}

	s.Log.WithField("request_id", requestID).Infof("validation '%s' created", req.Validation.XId)

	return &protos.CreateValidationResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Validation saved",
			RequestId: requestID,
		},
		Validation: req.Validation,
	}, nil
}

func (s *Server) UpdateValidation(ctx context.Context, req *protos.UpdateValidationRequest) (*protos.UpdateValidationResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	requestID := uuid.NewV4().String()

	validation := s.PersistentConfig.GetValidation(req.Id)
	if validation == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "validation not found")
	}

	// Deny ID changes
	req.Validation.XId = validation.XId

	if err := s.persistValidation(ctx, req.Validation); err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	// Publish UpdateValidation event for other plumber instances to receive
	if err := s.Bus.PublishUpdateValidation(ctx, req.Validation); err != nil {
		s.Log.Error(err)
	}

	s.Log.WithField("request_id", requestID).Infof("validation '%s' updated", req.Validation.XId)

	return &protos.UpdateValidationResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Validation updated",
			RequestId: requestID,
		},
		Validation: req.Validation,
	}, nil
}
func (s *Server) DeleteValidation(ctx context.Context, req *protos.DeleteValidationRequest) (*protos.DeleteValidationResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	requestID := uuid.NewV4().String()

	validation := s.PersistentConfig.GetValidation(req.Id)
	if validation == nil {
		return nil, CustomError(common.Code_NOT_FOUND, "validation not found")
	}

	if err := s.Bus.PublishDeleteValidation(ctx, validation); err != nil {
		s.Log.Errorf("unable to publish DeleteValidation message for '%s': %s", validation.XId, err)
		return nil, CustomError(common.Code_ABORTED, "DeleteValidation message could not be published")
	}

	s.PersistentConfig.DeleteValidation(validation.XId)
	s.PersistentConfig.Save()

	s.Log.WithField("request_id", requestID).Infof("validation '%s' updated", validation.XId)

	return &protos.DeleteValidationResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Validation deleted",
			RequestId: requestID,
		},
	}, nil
}

// persistValidation saves a validation to memory and etcd
func (s *Server) persistValidation(ctx context.Context, v *common.Validation) error {
	s.PersistentConfig.SetValidation(v.XId, v)
	s.PersistentConfig.Save()
	return nil
}
