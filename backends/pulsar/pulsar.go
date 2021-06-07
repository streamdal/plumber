package pulsar

import (
	"time"

	"github.com/batchcorp/plumber/printer"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
)

type Pulsar struct {
	Options *cli.Options
	Client  pulsar.Client
	MsgDesc *desc.MessageDescriptor
	log     *logrus.Entry
	printer printer.IPrinter
}

// NewClient creates a new pulsar client connection
func NewClient(opts *cli.Options) (pulsar.Client, error) {
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
