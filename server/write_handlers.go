package server

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber/validate"
	"github.com/batchcorp/plumber/writer"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
)

func (s *Server) Write(ctx context.Context, req *protos.WriteRequest) (*protos.WriteResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	if err := validate.WriteOptionsForServer(req.Opts); err != nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, fmt.Sprintf("unable to validate write options: %s", err))
	}

	be := s.PersistentConfig.GetBackend(req.Opts.ConnectionId)
	if be == nil {
		return nil, validate.ErrBackendNotFound
	}

	// We only need/want to do this once, so generate and pass to generateWriteValue

	md, err := s.getMessageDescriptorFromEncodeOptions(req.Opts.EncodeOptions)
	if err != nil {
		return nil, CustomError(common.Code_INTERNAL, fmt.Sprintf("unable to fetch message descriptor: %s", err))
	}

	records, err := writer.GenerateWriteValue(req.Opts, md)
	if err != nil {
		return nil, CustomError(common.Code_INTERNAL, fmt.Sprintf("unable to generate write records: %s", err))
	}

	// TODO: Should update to use a proper error chan
	if err := be.Write(ctx, req.Opts, nil, records...); err != nil {
		err = errors.Wrap(err, "unable to write messages to kafka")
		s.Log.Error(err)

		return &protos.WriteResponse{
			Status: &common.Status{
				Code:      common.Code_DATA_LOSS,
				Message:   err.Error(),
				RequestId: uuid.NewV4().String(),
			},
		}, nil
	}

	logMsg := fmt.Sprintf("wrote %d record(s)", len(records))

	s.Log.Debug(logMsg)

	return &protos.WriteResponse{
		Status: &common.Status{
			Code:      common.Code_OK,
			Message:   logMsg,
			RequestId: uuid.NewV4().String(),
		},
	}, nil
}
