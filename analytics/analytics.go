package analytics

import (
	"fmt"
	"net/http"
	"runtime"

	"github.com/batchcorp/plumber/options"
	"github.com/dukex/mixpanel"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

const (
	MixPanelAPIURL = "https://api.mixpanel.com"
)

type IAnalytics interface {
	Track(distinctId, eventName string, e *mixpanel.Event) error
	AsyncTrack(distinctId, eventName string, e *mixpanel.Event)
}

type Config struct {
	HTTPClient *http.Client
	Token      string
	PlumberID  string
}

type Analytics struct {
	Client mixpanel.Mixpanel
	cfg    *Config
	log    *logrus.Entry
}

type NoopAnalytics struct{}

func New(cfg *Config) (*Analytics, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to validate config")
	}

	var client mixpanel.Mixpanel

	if cfg.HTTPClient != nil {
		client = mixpanel.NewFromClient(cfg.HTTPClient, cfg.Token, MixPanelAPIURL)
	} else {
		logrus.Info("Doing default http client for mixpanel")
		client = mixpanel.New(cfg.Token, MixPanelAPIURL)
	}

	return &Analytics{
		Client: client,
		cfg:    cfg,
		log:    logrus.WithField("pkg", "analytics"),
	}, nil
}

func validateConfig(cfg *Config) error {
	if cfg == nil {
		return errors.New("config cannot be nil")
	}

	if cfg.Token == "" {
		return errors.New("config.Token cannot be empty")
	}

	if cfg.PlumberID == "" {
		return errors.New("config.PlumberID cannot be empty")
	}

	return nil
}

func (a *Analytics) AsyncTrack(distinctId, eventName string, e *mixpanel.Event) {
	go func() {
		if err := a.Track(distinctId, eventName, e); err != nil {
			a.log.Warningf("unable to track analytic event: %s", err)
		}
	}()
}

func (a *Analytics) Track(distinctId, eventName string, e *mixpanel.Event) error {
	if eventName == "" || e == nil {
		err := errors.New("eventName and e cannot be empty")
		a.log.Warningf("unable to track analytic event: %s", err)

		return err
	}

	if distinctId == "" {
		distinctId = uuid.NewV4().String()
	}

	if e.Properties == nil {
		e.Properties = make(map[string]interface{})
	}

	if _, ok := e.Properties["version"]; !ok {
		e.Properties["version"] = options.VERSION
	}

	if _, ok := e.Properties["os"]; !ok {
		e.Properties["os"] = runtime.GOOS
	}

	if _, ok := e.Properties["arch"]; !ok {
		e.Properties["arch"] = runtime.GOARCH
	}

	if _, ok := e.Properties["plumber_id"]; !ok {
		e.Properties["plumber_id"] = a.cfg.PlumberID
	}

	fmt.Println("performing a write for event ", eventName)

	err := a.Client.Track(distinctId, eventName, e)
	if err != nil {
		a.log.Warningf("unable to send event to mixpanel: %s", err)
		return errors.Wrap(err, "unable to send event to mixpanel")
	}

	return nil
}

func (a *NoopAnalytics) Track(_, _ string, _ *mixpanel.Event) error {
	return nil
}

func (a *NoopAnalytics) AsyncTrack(_, _ string, _ *mixpanel.Event) {}
