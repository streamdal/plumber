package server

import (
	"context"
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber/backends"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/validate"
	"github.com/batchcorp/plumber/writer"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/encoding"
)

func (s *Server) Write(ctx context.Context, req *protos.WriteRequest) (*protos.WriteResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	if err := validate.WriteOptionsForServer(req.Opts); err != nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, fmt.Sprintf("unable to validate write options: %s", err))
	}

	conn := s.PersistentConfig.GetConnection(req.Opts.ConnectionId)
	if conn == nil {
		return nil, CustomError(common.Code_NOT_FOUND, validate.ErrConnectionNotFound.Error())
	}

	be, err := backends.New(conn)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, fmt.Sprintf("unable to create backend: %s", err))
	}

	defer be.Close(ctx)

	// We only need/want to do this once, so generate and pass to generateWriteValue

	var md *desc.MessageDescriptor

	if req.Opts.EncodeOptions != nil && req.Opts.EncodeOptions.EncodeType == encoding.EncodeType_ENCODE_TYPE_JSONPB {
		var mdErr error

		cachedSchemaOptions := s.PersistentConfig.GetSchema(req.Opts.EncodeOptions.SchemaId)

		md, mdErr = pb.GetMessageDescriptor(cachedSchemaOptions, req.Opts.EncodeOptions.ProtobufSettings)
		if mdErr != nil {
			return nil, CustomError(common.Code_INTERNAL, fmt.Sprintf("unable to fetch message descriptor: %s", mdErr))
		}
	}

	// Okay if md is nil
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
