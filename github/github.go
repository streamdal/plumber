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
	GetAccessToken(response *UserCodeResponse) (*AccessTokenResponse, error)
	GetRepoArchive(ctx context.Context, token, repoURL string) ([]byte, error)
	GetUserCode() (*UserCodeResponse, error)
	PollForAccessToken(cfg *UserCodeResponse) (string, error)
	Post(url string, values url.Values) ([]byte, int, error)
}

type Github struct {
	Client        *http.Client
	oAuthClientID string
	log           *logrus.Entry
}

type UserCodeResponse struct {
	DeviceCode      string
	UserCode        string
	VerificationURL string
	ExpiresIn       time.Time
	CheckInterval   time.Duration
}

type AccessTokenResponse struct {
	BearerToken   string
	SleepDuration time.Duration
}

// New returns a configured Github struct
func New() (*Github, error) {
	return &Github{
		Client:        &http.Client{},
		oAuthClientID: "ea7ea641fb2ac352455f", // Static value, won't change
		log:           logrus.WithField("pkg", "github/github_handlers.go"),
	}, nil
}

// GetUserCode is the first method to be called in a github authorization flow. This will return back
// both a user code and a device code. The user code should be presented to the user and the user directed
// to  UserCodeResponse.VerificationURL to enter in the authorization code
func (g *Github) GetUserCode() (*UserCodeResponse, error) {
	values := url.Values{
		"client_id": {g.oAuthClientID},
		"scope":     {"repo"},
	}
	res, code, err := g.Post("https://github.com/login/device/code", values)
	if err != nil {
		return nil, errors.Wrap(err, "unable to call Github API")
	}

	if code < 200 || code > 299 {
		return nil, fmt.Errorf("non-200 response: %d", code)
	}

	result, err := url.ParseQuery(string(res))
	if err != nil {
		return nil, errors.Wrap(err, "could not parse Github response")
	}

	g.log.Debugf("GH response: %+v", result)

	expiresDuration, err := time.ParseDuration(result.Get("expires_in") + "s")
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse expiration time of Github response")
	}

	checkInterval, err := time.ParseDuration(result.Get("interval") + "s")
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse authorization check interval")
	}

	return &UserCodeResponse{
		DeviceCode:      result.Get("device_code"),
		UserCode:        result.Get("user_code"),
		VerificationURL: result.Get("verification_uri"),
		ExpiresIn:       time.Now().UTC().Add(expiresDuration),
		CheckInterval:   checkInterval,
	}, nil
}

// PollForAccessToken is called after a user_code
func (g *Github) PollForAccessToken(cfg *UserCodeResponse) (string, error) {
	ctx, cancel := context.WithDeadline(context.Background(), cfg.ExpiresIn)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			g.log.Error(ErrVerifyTimeout)
			return "", ErrVerifyTimeout
		default:
			// NOOP
		}

		tokenResponse, err := g.GetAccessToken(cfg)
		if err != nil {
			return "", errors.Wrap(err, "unable to get GitHub bearer token")
		}

		if tokenResponse.BearerToken == "" {
			g.log.Debug("Still waiting on user to enter auth code")
			time.Sleep(tokenResponse.SleepDuration)
			continue
		}

		return tokenResponse.BearerToken, nil
	}
}

// GetAccessToken attempts to get a bearer token from GitHub. If the user has not entered the token into
// github's verification page yet, a sleep interval will be set on the return value and we should continue to
// poll every few seconds. Once the user enters the code, this method will return the bearer token that can
// be used to call GitHub API endpoints
func (g *Github) GetAccessToken(cfg *UserCodeResponse) (*AccessTokenResponse, error) {
	values := url.Values{
		"client_id":   {g.oAuthClientID},
		"grant_type":  {"urn:ietf:params:oauth:grant-type:device_code"},
		"device_code": {cfg.UserCode},
	}

	res, code, err := g.Post("https://github.com/login/oauth/access_token", values)
	if err != nil {
		return nil, errors.Wrap(err, "unable to call Github API")
	}

	if code < 200 || code > 299 {
		return nil, fmt.Errorf("non-200 response: %d", code)
	}

	result, err := url.ParseQuery(string(res))
	if err != nil {
		return nil, errors.Wrap(err, "could not parse Github response")
	}

	if result.Get("error") == "authorization_pending" {
		return &AccessTokenResponse{
			SleepDuration: cfg.CheckInterval,
		}, nil
	} else if result.Get("error") == "slow_down" {
		sleepDuration, err := time.ParseDuration(result.Get("interval") + "s")
		if err != nil {
			return &AccessTokenResponse{
				SleepDuration: time.Second * 30,
			}, nil
		}

		return &AccessTokenResponse{
			SleepDuration: sleepDuration,
		}, nil
	}

	return &AccessTokenResponse{
		BearerToken: result.Get("access_token"),
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
