package server

import (
	"context"
	"fmt"

	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/actions"
	"github.com/batchcorp/plumber/bus"
	"github.com/batchcorp/plumber/config"
	"github.com/batchcorp/plumber/validate"
)

type Server struct {
	Actions          actions.IActions
	AuthToken        string
	PersistentConfig *config.Config
	Bus              bus.IBus
	Log              *logrus.Entry
	CLIOptions       *opts.CLIOptions
}

func (s *Server) GetServerOptions(_ context.Context, req *protos.GetServerOptionsRequest) (*protos.GetServerOptionsResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	return &protos.GetServerOptionsResponse{
		ServerOptions: &opts.ServerOptions{
			NodeId:            s.CLIOptions.Server.NodeId,
			ClusterId:         s.CLIOptions.Server.ClusterId,
			GrpcListenAddress: s.CLIOptions.Server.GrpcListenAddress,
			AuthToken:         s.CLIOptions.Server.AuthToken,
		},
	}, nil
}

type ErrorWrapper struct {
	Status *common.Status
}

func (e *ErrorWrapper) Error() string {
	return e.Status.Message
}

func CustomError(c common.Code, msg string) error {
	return &ErrorWrapper{
		Status: &common.Status{
			Code:      c,
			Message:   msg,
			RequestId: uuid.NewV4().String(),
		},
	}
}

func (s *Server) validateAuth(auth *common.Auth) error {
	if auth == nil {
		return validate.ErrMissingAuth
	}

	if auth.Token != s.AuthToken {
		return validate.ErrInvalidToken
	}

	return nil
}
