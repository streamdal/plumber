package pulsar

import (
	"time"

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
}

func NewClient(opts *cli.Options) (pulsar.Client, error) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               opts.Pulsar.Address,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		return nil, errors.Wrap(err, "Could not instantiate Pulsar client")
	}

	return client, nil
}
