package server

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/embed/etcd"
	"github.com/batchcorp/plumber/validate"
)

func (s *Server) GetAllComposites(ctx context.Context, req *protos.GetAllCompositesRequest) (*protos.GetAllCompositesResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	composites := make([]*opts.Composite, 0)

	for _, composite := range s.PersistentConfig.Composites {
		composites = append(composites, composite)
	}

	return &protos.GetAllCompositesResponse{
		Composites: composites,
	}, nil
}

func (s *Server) GetComposite(ctx context.Context, req *protos.GetCompositeRequest) (*protos.GetCompositeResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	composite := s.PersistentConfig.GetComposite(req.Id)
	if composite == nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, validate.ErrCompositeNotFound.Error())
	}

	return &protos.GetCompositeResponse{
		Composite: composite,
	}, nil
}

func (s *Server) CreateComposite(ctx context.Context, req *protos.CreateCompositeRequest) (*protos.CreateCompositeResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	composite := req.Composite

	if err := validate.CompositeOptionsForServer(composite); err != nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, err.Error())
	}

	composite.XId = uuid.NewV4().String()

	s.PersistentConfig.SetComposite(composite.XId, composite)

	data, err := proto.Marshal(composite)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, "could not marshal connection")
	}

	_, err = s.Etcd.Put(ctx, etcd.CacheCompositesPrefix+"/"+composite.XId, string(data))
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	if err := s.Etcd.PublishCreateComposite(ctx, composite); err != nil {
		s.rollbackCreateComposite(ctx, composite)

		s.Log.Error(errors.Wrap(err, "unable to publish create connection event"))
		return nil, CustomError(common.Code_INTERNAL, fmt.Sprintf("unable to create connection event: %s", err))
	}

	s.Log.Infof("Composite view '%s' created", composite.XId)

	return &protos.CreateCompositeResponse{
		Composite: composite,
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Composite view created",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (s *Server) UpdateComposite(ctx context.Context, req *protos.UpdateCompositeRequest) (*protos.UpdateCompositeResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	composite := s.PersistentConfig.GetComposite(req.Id)
	if composite == nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, validate.ErrCompositeNotFound.Error())
	}

	if err := validate.CompositeOptionsForServer(req.Composite); err != nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, err.Error())
	}

	composite.ReadIds = req.Composite.ReadIds

	s.PersistentConfig.SetComposite(composite.XId, composite)

	data, err := proto.Marshal(composite)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, "could not marshal connection")
	}

	_, err = s.Etcd.Put(ctx, etcd.CacheCompositesPrefix+"/"+composite.XId, string(data))
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	if err := s.Etcd.PublishUpdateComposite(ctx, composite); err != nil {
		s.rollbackCreateComposite(ctx, composite)

		s.Log.Error(errors.Wrap(err, "unable to publish update composite event"))
		return nil, CustomError(common.Code_INTERNAL, fmt.Sprintf("unable to update composite event: %s", err))
	}

	s.Log.Infof("Composite view '%s' created", composite.XId)

	return &protos.UpdateCompositeResponse{
		Composite: composite,
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Composite view updated",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

func (s *Server) DeleteComposite(ctx context.Context, req *protos.DeleteCompositeRequest) (*protos.DeleteCompositeResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	composite := s.PersistentConfig.GetComposite(req.Id)

	if composite == nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, validate.ErrCompositeNotFound.Error())
	}

	_, err := s.Etcd.Delete(ctx, etcd.CacheCompositesPrefix+"/"+composite.XId)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	if err := s.Etcd.PublishDeleteComposite(ctx, composite); err != nil {
		s.Log.Error(errors.Wrap(err, "unable to publish delete composite event"))
		return nil, CustomError(common.Code_INTERNAL, fmt.Sprintf("unable to delete composite event: %s", err))
	}

	s.PersistentConfig.DeleteComposite(composite.XId)

	s.Log.Debugf("Composite view '%s' deleted", composite.XId)

	return &protos.DeleteCompositeResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   "Composite view deleted",
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}

// Rollback anything that may have been done during a composite creation request
func (s *Server) rollbackCreateComposite(ctx context.Context, composite *opts.Composite) {
	// Remove composite view from etcd
	if _, err := s.Etcd.Delete(ctx, etcd.CacheCompositesPrefix+"/"+composite.XId); err != nil {
		s.Log.Errorf("unable to delete composite view in etcd: %s", err)
	}

	// Delete composite cache map entry
	s.PersistentConfig.DeleteComposite(composite.XId)
}
