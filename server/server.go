package server

import (
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

func (p *Server) validateRequest(auth *common.Auth) error {
	if auth == nil {
		return ErrMissingAuth
	}

	if auth.Token != p.AuthToken {
		return ErrInvalidToken
	}

	return nil
}
