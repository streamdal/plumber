package server

import (
	"context"
	"fmt"

	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber/github"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
)

func (s *Server) GetRepoTree(ctx context.Context, req *protos.GetRepoTreeRequest) (*protos.GetRepoTreeResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	repo, err := parseRepoURL(req.RepoUrl)
	if err != nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, err.Error())
	}

	tree, err := s.GithubService.GetRepoTree(ctx, s.PersistentConfig.GitHubToken, repo.Organization, repo.Name)
	if err != nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, err.Error())
	}

	return &protos.GetRepoTreeResponse{
		Tree: github.TreeToDisplay(tree.Entries, protos.SchemaType_SCHEMA_TYPE_UNSET),
	}, nil
}

func (s *Server) GetRepoFile(ctx context.Context, req *protos.GetRepoFileRequest) (*protos.GetRepoFileResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	repo, err := parseRepoURL(req.RepoUrl)
	if err != nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, err.Error())
	}

	content, err := s.GithubService.GetRepoFile(ctx, s.PersistentConfig.GitHubToken, repo.Organization, repo.Name, req.FileSha)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	return &protos.GetRepoFileResponse{
		Content: content,
	}, nil
}

func (s *Server) CreatePullRequest(ctx context.Context, req *protos.CreatePRRequest) (*protos.CreatePRResponse, error) {
	if err := s.validateAuth(req.Auth); err != nil {
		return nil, CustomError(common.Code_UNAUTHENTICATED, fmt.Sprintf("invalid auth: %s", err))
	}

	repo, err := parseRepoURL(req.RepoUrl)
	if err != nil {
		return nil, CustomError(common.Code_FAILED_PRECONDITION, err.Error())
	}

	files := make([]*github.PullRequestFile, 0)
	for _, f := range req.Files {
		files = append(files, &github.PullRequestFile{
			Path:    f.Path,
			Content: string(f.Contents),
		})
	}

	opts := &github.PullRequestOpts{
		BranchName: req.BranchName,
		CommitName: "Plumber Commit",
		Title:      req.PrName,
		Body:       req.PrBody,
		Files:      files,
	}

	pr, err := s.GithubService.CreatePullRequest(ctx, s.PersistentConfig.GitHubToken, repo.Organization, repo.Name, opts)
	if err != nil {
		return nil, CustomError(common.Code_ABORTED, err.Error())
	}

	return &protos.CreatePRResponse{
		Url: pr.GetHTMLURL(),
	}, nil
}
