package pulsar

import (
	"time"

	"github.com/batchcorp/plumber/printer"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/options"
)

type Pulsar struct {
	Options *options.Options

	client   pulsar.Client
	producer pulsar.Producer
	msgDesc  *desc.MessageDescriptor
	log      *logrus.Entry
	printer  printer.IPrinter
}

func New(opts *options.Options) (*Pulsar, error) {
	if err := validateOpts(opts); err != nil {
		return nil, errors.Wrap(err, "unable to validate options")
	}

	return &Pulsar{
		Options: opts,
		log:     logrus.WithField("backend", "pulsar"),
	}, nil
}

// TODO: Implement
func validateOpts(opts *options.Options) error {
	return nil
}

// NewClient creates a new pulsar client connection
func NewClient(opts *options.Options) (pulsar.Client, error) {
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
