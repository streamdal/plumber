package kubemq_queue

import (
	"context"
	"github.com/batchcorp/plumber/cli"
	"github.com/jhump/protoreflect/desc"
	"github.com/kubemq-io/kubemq-go/queues_stream"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"net"
	"strconv"
)

var (
	errMissingQueue = errors.New("you must specify a queue name to publish to")
)

type KubeMQQueue struct {
	Options *cli.Options
	Client  *queues_stream.QueuesStreamClient
	Context context.Context
	MsgDesc *desc.MessageDescriptor
	log     *logrus.Entry
}

func NewClient(opts *cli.Options) (*queues_stream.QueuesStreamClient, error) {
	host, portStr, err := net.SplitHostPort(opts.KubeMQQueue.Address)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}
	if opts.KubeMQQueue.ClientID == "" {
		opts.KubeMQQueue.ClientID = "plumber-client"
	}
	if opts.KubeMQQueue.TLSCertFile == "" {
		return queues_stream.NewQueuesStreamClient(context.Background(),
			queues_stream.WithAddress(host, port),
			queues_stream.WithClientId(opts.KubeMQQueue.ClientID),
			queues_stream.WithAuthToken(opts.KubeMQQueue.AuthToken))
	} else {
		return queues_stream.NewQueuesStreamClient(context.Background(),
			queues_stream.WithAddress(host, port),
			queues_stream.WithClientId(opts.KubeMQQueue.ClientID),
			queues_stream.WithCredentials(opts.KubeMQQueue.TLSCertFile, ""),
			queues_stream.WithAuthToken(opts.KubeMQQueue.AuthToken))
	}
}
