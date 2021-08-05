package github

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/pkg/errors"

	"github.com/sirupsen/logrus"
)

var (
	ErrPendingAuth = errors.New("pending authorization")
)

type IGithub interface {
	GetAccessToken(deviceCode string) (string, error)
	GetUserCode() (*UserCodeResponse, error)
	PollForAccessToken(cfg *UserCodeResponse) (string, error)
	Post(url string, params map[string]interface{}) ([]byte, int, error)
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
}

// New returns a configured Github struct
func New() (*Github, error) {
	return &Github{
		Client:        &http.Client{},
		oAuthClientID: "ea7ea641fb2ac352455f", // Static value, won't change
		log:           logrus.WithField("pkg", "github/github.go"),
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

	return &UserCodeResponse{
		DeviceCode:      result.Get("device_code"),
		UserCode:        result.Get("user_code"),
		VerificationURL: result.Get("verification_uri"),
		ExpiresIn:       time.Now().UTC().Add(expiresDuration),
	}, nil
}

// PollForAccessToken is called after a user_code
func (g *Github) PollForAccessToken(cfg *UserCodeResponse) (string, error) {
	ctx, cancel := context.WithDeadline(context.Background(), cfg.ExpiresIn)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			err := errors.New("Unable to verify GitHub access after 15 minutes")
			g.log.Error(err)
			return "", err
		default:
			// NOOP
		}

		bearerToken, err := g.GetAccessToken(cfg.DeviceCode)
		if err == ErrPendingAuth {
			g.log.Debug("Still waiting on user to enter auth code")
			time.Sleep(time.Second * 5)
			continue
		}
		if err != nil {
			return "", errors.Wrap(err, "unable to get GitHub bearer token")
		}

		return bearerToken, nil
	}
}

// GetAccessToken attempts to get a bearer token from GitHub. If the user has not entered the token into
// github's verification page yet, a ErrPendingAuth error will be returned and we should continue to poll
// every few seconds. Once the user enters the code, this method will return the bearer token that can
// be used to call GitHub API endpoints
func (g *Github) GetAccessToken(deviceCode string) (string, error) {
	values := url.Values{
		"client_id":   {g.oAuthClientID},
		"grant_type":  {"urn:ietf:params:oauth:grant-type:device_code"},
		"device_code": {deviceCode},
	}

	res, code, err := g.Post("https://github.com/login/oauth/access_token", values)
	if err != nil {
		return "", errors.Wrap(err, "unable to call Github API")
	}

	if code < 200 || code > 299 {
		return "", fmt.Errorf("non-200 response: %d", code)
	}

	result, err := url.ParseQuery(string(res))
	if err != nil {
		return "", errors.Wrap(err, "could not parse Github response")
	}

	if result.Get("error") == "authorization_pending" {
		return "", ErrPendingAuth
	} else if result.Get("error") == "slow_down" {
		sleepDuration, err := time.ParseDuration(result.Get("interval") + "s")
		if err != nil {
			time.Sleep(time.Second * 30)
			return "", ErrPendingAuth
		}
		time.Sleep(sleepDuration)
		return "", ErrPendingAuth
	}

	return result.Get("access_token"), nil
}

// Post makes a form post and returns the resulting body contents
func (g *Github) Post(url string, values url.Values) ([]byte, int, error) {
	resp, err := http.PostForm(url, values)
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
