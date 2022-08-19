package telemetry

import (
	"net/http"
	"runtime"

	"github.com/batchcorp/plumber/options"
	"github.com/pkg/errors"
	"github.com/posthog/posthog-go"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

const (
	APIURL = "https://api.batch.sh"
	APIKey = "<fill in>"
)

type ITelemetry interface {
	Enqueue(c posthog.Capture) error
	AsyncEnqueue(c posthog.Capture)
}

type Config struct {
	Token        string
	PlumberID    string
	RoundTripper http.RoundTripper
}

type Telemetry struct {
	Client posthog.Client
	cfg    *Config
	log    *logrus.Entry
}

type NoopTelemtry struct{}

func New(cfg *Config) (*Telemetry, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to validate config")
	}

	client, err := posthog.NewWithConfig(APIKey, posthog.Config{
		Endpoint:  APIURL,
		Transport: cfg.RoundTripper,
	})

	if err != nil {
		return nil, errors.Wrap(err, "unable to create telemetry client")
	}

	client.Enqueue(posthog.Capture{})

	return &Telemetry{
		Client: client,
		cfg:    cfg,
		log:    logrus.WithField("pkg", "telemetry"),
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

func (t *Telemetry) Enqueue(c posthog.Capture) error {
	if c.Event == "" {
		err := errors.New("Event cannot be empty")
		t.log.Warningf("unable to track analytic event: %s", err)

		return err
	}

	if c.DistinctId == "" {
		c.DistinctId = uuid.NewV4().String()
	}

	if c.Properties == nil {
		c.Properties = make(map[string]interface{})
	}

	if _, ok := c.Properties["version"]; !ok {
		c.Properties["version"] = options.VERSION
	}

	if _, ok := c.Properties["os"]; !ok {
		c.Properties["os"] = runtime.GOOS
	}

	if _, ok := c.Properties["arch"]; !ok {
		c.Properties["arch"] = runtime.GOARCH
	}

	if _, ok := c.Properties["plumber_id"]; !ok {
		c.Properties["plumber_id"] = t.cfg.PlumberID
	}

	err := t.Client.Enqueue(c)
	if err != nil {
		t.log.Warningf("unable to send telemetry event: %s", err)
		return errors.Wrap(err, "unable to send telemetry event")
	}

	return nil
}

func (t *Telemetry) AsyncEnqueue(c posthog.Capture) {
	go t.Enqueue(c)
}

func (t *NoopTelemtry) Enqueue(_ posthog.Capture) error {
	return nil
}

func (t *NoopTelemtry) AsyncEnqueue(_ posthog.Capture) {}
