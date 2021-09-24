package github

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/go-github/v37/github"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/oauth2"
)

var (
	ErrVerifyTimeout = errors.New("Unable to verify GitHub access after 15 minutes")
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 . IGithub
type IGithub interface {
	GetRepoArchive(ctx context.Context, token, repoURL string) ([]byte, error)
	Post(url string, values url.Values) ([]byte, int, error)
}

type Github struct {
	Client *http.Client
	log    *logrus.Entry
}

type AccessTokenResponse struct {
	BearerToken   string
	SleepDuration time.Duration
}

// New returns a configured Github struct
func New() (*Github, error) {
	return &Github{
		Client: &http.Client{},
		log:    logrus.WithField("pkg", "github/github_handlers.go"),
	}, nil
}

// Post makes a form post and returns the resulting body contents
func (g *Github) Post(url string, values url.Values) ([]byte, int, error) {
	resp, err := g.Client.PostForm(url, values)
	if err != nil {
		return nil, 0, fmt.Errorf("API call to %s failed: %s", url, err)
	}
	defer resp.Body.Close()

	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}

	return contents, resp.StatusCode, nil
}

// GetRepoArchive returns a zip of the contents of a github repository
func (g *Github) GetRepoArchive(ctx context.Context, token, repoURL string) ([]byte, error) {
	owner, repo, err := parseRepoUrl(repoURL)
	if err != nil {
		return nil, err
	}

	archiveURL, err := getRepoArchiveLink(ctx, token, owner, repo)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to import '%s'", repoURL)
	}

	// Download contents
	resp, err := g.Client.Get(archiveURL.String())
	if err != nil {
		return nil, errors.Wrapf(err, "unable to import '%s'", repoURL)
	}

	defer resp.Body.Close()

	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to import '%s'", repoURL)
	}

	return contents, nil
}

// parseRepoUrl extracts the repo name and owner name from a github URL
func parseRepoUrl(in string) (string, string, error) {
	parts, err := url.Parse(in)
	if err != nil {
		return "", "", errors.Wrap(err, "unable to parse GitHub repository URL")
	}
	names := strings.Split(strings.TrimLeft(parts.Path, "/"), "/")
	return names[0], names[1], nil
}

// getRepoArchiveLink queries github's API for the link to download a zip of the repository
func getRepoArchiveLink(ctx context.Context, token, owner, repo string) (*url.URL, error) {
	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: token},
	)

	tc := oauth2.NewClient(ctx, ts)

	client := github.NewClient(tc)

	archiveURL, _, err := client.Repositories.GetArchiveLink(ctx, owner, repo, github.Zipball, nil, true)
	if err != nil {
		return nil, err
	}

	return archiveURL, nil
}
