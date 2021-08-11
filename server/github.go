package server

import (
	"context"
	"fmt"
	"time"

	"github.com/batchcorp/plumber-schemas/build/go/protos/common"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
)

// StartGithubAuth returns a user code and authorization URL that users need to go to and enter in the code in
// order to authorize API access for Plumber
func (p *PlumberServer) StartGithubAuth(_ context.Context, req *protos.StartGithubAuthRequest) (*protos.StartGithubAuthResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	cfg, err := p.GithubService.GetUserCode()
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	p.GithubAuth = cfg

	return &protos.StartGithubAuthResponse{
		Code:            cfg.UserCode,
		VerificationUrl: cfg.VerificationURL,
		EnterBefore:     cfg.ExpiresIn.Format(time.RFC3339),
	}, nil
}

// PollGithubAuth starts a poll of the github authorization status
func (p *PlumberServer) PollGithubAuth(req *protos.PollGithubAuthRequest, srv protos.PlumberServer_PollGithubAuthServer) error {
	if err := p.validateRequest(req.Auth); err != nil {
		return CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	if p.GithubAuth == nil {
		return CustomError(common.Code_FAILED_PRECONDITION, "no github authorization is pending, call StartGithubAuth() first")
	}

	cfg := p.GithubAuth

	ctx, cancel := context.WithDeadline(context.Background(), cfg.ExpiresIn)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			p.Log.Error("Unable to verify GitHub access after 15 minutes")
			srv.Send(&protos.PollGithubAuthResponse{
				Status: protos.PollGithubAuthResponse_FAILED,
			})
		default:
			// NOOP
		}

		resp, err := p.GithubService.GetAccessToken(cfg)
		if resp.BearerToken == "" {
			srv.Send(&protos.PollGithubAuthResponse{
				Status: protos.PollGithubAuthResponse_PENDING,
			})
			time.Sleep(cfg.CheckInterval)
			continue
		}
		if err != nil {
			return CustomError(common.Code_ABORTED, err.Error())
		}

		p.PersistentConfig.GitHubToken = resp.BearerToken
		p.PersistentConfig.Save()

		srv.Send(&protos.PollGithubAuthResponse{
			Status: protos.PollGithubAuthResponse_SUCCESS,
		})

		return nil
	}
}

// IsGithubAuth determines if we have authorized plumber with github
func (p *PlumberServer) IsGithubAuth(_ context.Context, req *protos.IsGithubAuthRequest) (*protos.IsGithubAuthResponse, error) {
	if err := p.validateRequest(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	return &protos.IsGithubAuthResponse{
		Authorized: p.PersistentConfig.GitHubToken != "",
	}, nil
}
