package telemetry

import (
	"net/http"
	"runtime"
	"time"

	"github.com/pkg/errors"
	"github.com/posthog/posthog-go"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/options"
)

const (
	APIURL = "https://telemetry.streamdal.com"
)

type ITelemetry interface {
	Enqueue(c posthog.Capture) error
}

type Config struct {
	Token      string
	PlumberID  string
	CLIOptions *opts.CLIOptions

	// optional
	RoundTripper http.RoundTripper
}

type Telemetry struct {
	Client posthog.Client
	cfg    *Config
	log    *logrus.Entry
}

type NoopTelemetry struct{}

type NoopLogger struct{}

func New(cfg *Config) (*Telemetry, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "unable to validate config")
	}

	client, err := posthog.NewWithConfig(cfg.Token, posthog.Config{
		Endpoint:  APIURL,
		Transport: cfg.RoundTripper, // posthog lib will instantiate a default roundtripper if nil
		BatchSize: 1,
		Interval:  250 * time.Millisecond,
		Logger:    &NoopLogger{},
	})

	if err != nil {
		return nil, errors.Wrap(err, "unable to create telemetry client")
	}

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

	if cfg.CLIOptions == nil {
		return errors.New("CLIOptions cannot be nil")
	}

	return nil
}

func (t *Telemetry) Enqueue(c posthog.Capture) error {
	if c.Event == "" {
		err := errors.New("Event cannot be empty")
		t.log.Warningf("unable to track analytic event: %s", err)

		return err
	}

	// This should _usually_ be already set to plumber ID
	if c.DistinctId == "" {
		c.DistinctId = uuid.NewV4().String()
	}

	if c.Properties == nil {
		c.Properties = make(map[string]interface{})
	}

	if _, ok := c.Properties["debug"]; !ok {
		c.Properties["debug"] = t.cfg.CLIOptions.Global.Debug
	}

	if _, ok := c.Properties["quiet"]; !ok {
		c.Properties["debug"] = t.cfg.CLIOptions.Global.Quiet
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

func (t *NoopTelemetry) Enqueue(_ posthog.Capture) error {
	return nil
}

func (l *NoopLogger) Logf(format string, args ...interface{}) {
	// NOOP
}

func (l *NoopLogger) Errorf(format string, args ...interface{}) {
	// NOOP
}
