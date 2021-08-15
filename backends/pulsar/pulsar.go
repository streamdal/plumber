package pulsar

import (
	"context"
	"time"

	"github.com/batchcorp/plumber/types"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
)

type Pulsar struct {
	Options *options.Options

	client   pulsar.Client
	producer pulsar.Producer
	log      *logrus.Entry
}

func New(opts *options.Options) (*Pulsar, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	client, err := newClient(opts)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new client")
	}

	return &Pulsar{
		Options: opts,
		client:  client,
		log:     logrus.WithField("backend", "pulsar"),
	}, nil
}

func (p *Pulsar) Close(ctx context.Context) error {
	if p.client == nil {
		return nil
	}

	p.client.Close()

	p.client = nil

	return nil
}

func (p *Pulsar) Test(ctx context.Context) error {
	return types.NotImplementedErr
}

func (p *Pulsar) Dynamic(ctx context.Context) error {
	return types.UnsupportedFeatureErr
}

func (p *Pulsar) Lag(ctx context.Context) (*types.Lag, error) {
	return nil, types.UnsupportedFeatureErr
}

func (p *Pulsar) Relay(ctx context.Context, relayCh chan interface{}, errorCh chan *types.ErrorMessage) error {
	return types.UnsupportedFeatureErr
}

// NewClient creates a new pulsar client connection
func newClient(opts *options.Options) (pulsar.Client, error) {
	clientOpts := pulsar.ClientOptions{
		URL:                        opts.Pulsar.Address,
		OperationTimeout:           30 * time.Second,
		ConnectionTimeout:          opts.Pulsar.ConnectTimeout,
		TLSAllowInsecureConnection: opts.Pulsar.InsecureTLS,
	}

	if opts.Pulsar.AuthCertificateFile != "" && opts.Pulsar.AuthKeyFile != "" {
		clientOpts.Authentication = pulsar.NewAuthenticationTLS(opts.Pulsar.AuthCertificateFile, opts.Pulsar.AuthKeyFile)
	}

	client, err := pulsar.NewClient(clientOpts)
	if err != nil {
		return nil, errors.Wrap(err, "Could not instantiate Pulsar client")
	}

	return client, nil
}

func validateOpts(opts *options.Options) error {
	if opts == nil {
		return errors.New("opts cannot be nil")
	}

	if opts.Pulsar == nil {
		return errors.New("Pulsar opts cannot be nil")
	}

	return nil
}
