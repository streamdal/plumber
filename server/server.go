package server

import (
	"context"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber/validate"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/common"

	"github.com/batchcorp/plumber/config"
	"github.com/batchcorp/plumber/embed/etcd"
	"github.com/batchcorp/plumber/github"
)

type Server struct {
	AuthToken        string
	PersistentConfig *config.Config
	GithubAuth       *github.UserCodeResponse
	GithubService    github.IGithub
	Etcd             etcd.IEtcd
	Log              *logrus.Entry
}

func (s *Server) SetServerOptions(ctx context.Context, request *protos.SetServerOptionsRequest) (*protos.SetServerOptionsResponse, error) {
	panic("implement me")
}

func (s *Server) GetVCEvents(request *protos.GetVCEventsRequest, server protos.PlumberServer_GetVCEventsServer) error {
	panic("implement me")
}

func (s *Server) GetServerOptions(ctx context.Context, request *protos.GetServerOptionsRequest) (*protos.GetServerOptionsResponse, error) {
	panic("implement me")
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
