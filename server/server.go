package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/lestrrat-go/jwx/jwt"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/config"
	"github.com/batchcorp/plumber/embed/etcd"
	"github.com/batchcorp/plumber/github"
	"github.com/batchcorp/plumber/validate"
	"github.com/batchcorp/plumber/vcservice"
)

type Server struct {
	AuthToken        string
	PersistentConfig *config.Config
	VCService        vcservice.IVCService
	GithubService    github.IGithub
	Etcd             etcd.IEtcd
	Log              *logrus.Entry
	CLIOptions       *opts.CLIOptions
}

func (s *Server) GetServerOptions(_ context.Context, req *protos.GetServerOptionsRequest) (*protos.GetServerOptionsResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	return &protos.GetServerOptionsResponse{
		ServerOptions: &opts.ServerOptions{
			NodeId:             s.CLIOptions.Server.NodeId,
			ClusterId:          s.CLIOptions.Server.ClusterId,
			GrpcListenAddress:  s.CLIOptions.Server.GrpcListenAddress,
			AuthToken:          s.CLIOptions.Server.AuthToken,
			InitialCluster:     s.CLIOptions.Server.InitialCluster,
			AdvertisePeerUrl:   s.CLIOptions.Server.AdvertisePeerUrl,
			AdvertiseClientUrl: s.CLIOptions.Server.AdvertiseClientUrl,
			ListenerPeerUrl:    s.CLIOptions.Server.ListenerPeerUrl,
			ListenerClientUrl:  s.CLIOptions.Server.ListenerClientUrl,
			PeerToken:          s.CLIOptions.Server.PeerToken,
		},
	}, nil
}

// SetServerOptions is called by the frontend to update any necessary server config options.
// These changes will also be broadcast to other plumber instances.
func (s *Server) SetServerOptions(ctx context.Context, req *protos.SetServerOptionsRequest) (*protos.SetServerOptionsResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	vcServiceJWT := req.GetVcserviceToken()
	if vcServiceJWT == "" {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, "VcserviceToken cannot be empty")
	}

	s.PersistentConfig.VCServiceToken = vcServiceJWT

	stateMap, err := decodeJWTState([]byte(vcServiceJWT))
	if err != nil {
		return nil, err
	}

	s.PersistentConfig.GitHubToken = stateMap["oauth_token_github"]

	// Save to etcd
	if err := s.Etcd.SaveConfig(ctx, s.PersistentConfig); err != nil {
		return nil, errors.Wrap(err, "unable to save updated config values")
	}

	msg := &etcd.MessageUpdateConfig{
		VCServiceToken: req.GetVcserviceToken(),
		GithubToken:    stateMap["oauth_token_github"],
	}

	if err := s.Etcd.PublishConfigUpdate(ctx, msg); err != nil {
		return nil, errors.Wrap(err, "unable to broadcast config update")
	}

	return &protos.SetServerOptionsResponse{
		ServerOptions: &opts.ServerOptions{
			NodeId:             s.CLIOptions.Server.NodeId,
			ClusterId:          s.CLIOptions.Server.ClusterId,
			GrpcListenAddress:  s.CLIOptions.Server.GrpcListenAddress,
			AuthToken:          s.CLIOptions.Server.AuthToken,
			InitialCluster:     s.CLIOptions.Server.InitialCluster,
			AdvertisePeerUrl:   s.CLIOptions.Server.AdvertisePeerUrl,
			AdvertiseClientUrl: s.CLIOptions.Server.AdvertiseClientUrl,
			ListenerPeerUrl:    s.CLIOptions.Server.ListenerPeerUrl,
			ListenerClientUrl:  s.CLIOptions.Server.ListenerClientUrl,
			PeerToken:          s.CLIOptions.Server.PeerToken,
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

// decodeJWTState decodes a vc-service token, which has a base64 encoded, JSON marshaled version of
// a map[string]string as the value of the "sub" key of the JWT.
func decodeJWTState(vcserviceJWT []byte) (map[string]string, error) {
	// Decode JWT to get oauth token
	jwtToken, err := jwt.Parse(vcserviceJWT)
	if err != nil {
		return nil, errors.Wrap(err, "invalid token")
	}

	anonStateData, ok := jwtToken.Get(jwt.SubjectKey)
	if !ok {
		return nil, errors.New("unable to find plumber cluster ID in state JWT")
	}

	stateData, ok := anonStateData.(string)
	if !ok {
		return nil, errors.New("unable to type assert state data from the JWT")
	}

	decoded, err := base64.StdEncoding.DecodeString(stateData)
	if err != nil {
		return nil, errors.Wrap(err, "unable to base64 decode JWT payload")
	}

	stateMap := make(map[string]string)
	if err := json.Unmarshal(decoded, &stateMap); err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal state")
	}

	return stateMap, nil
}
