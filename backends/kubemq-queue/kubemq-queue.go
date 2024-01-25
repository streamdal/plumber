package kubemq_queue

import (
	"context"
	"net"
	"strconv"

	queuesStream "github.com/kubemq-io/kubemq-go/queues_stream"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/streamdal/plumber/types"
)

const (
	DefaultReadTimeout = 10000 // Milliseconds
	BackendName        = "kubemq"
)

type KubeMQ struct {
	connOpts *opts.ConnectionOptions

	connArgs *args.KubeMQQueueConn

	client *queuesStream.QueuesStreamClient
	log    *logrus.Entry
}

func New(opts *opts.ConnectionOptions) (*KubeMQ, error) {

	args := opts.GetKubemqQueue()

	host, portStr, err := net.SplitHostPort(args.Address)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}

	var client *queuesStream.QueuesStreamClient

	if args.TlsClientCert == "" {
		client, err = queuesStream.NewQueuesStreamClient(context.Background(),
			queuesStream.WithAddress(host, port),
			queuesStream.WithClientId(args.ClientId),
			queuesStream.WithAuthToken(args.AuthToken))
	} else {
		client, err = queuesStream.NewQueuesStreamClient(context.Background(),
			queuesStream.WithAddress(host, port),
			queuesStream.WithClientId(args.ClientId),
			queuesStream.WithCredentials(args.TlsClientCert, ""),
			queuesStream.WithAuthToken(args.AuthToken))
	}
	if err != nil {

	}

	return &KubeMQ{
		connOpts: opts,
		connArgs: opts.GetKubemqQueue(),
		client:   client,
		log:      logrus.WithField("backend", BackendName),
	}, nil
}

func (k *KubeMQ) Name() string {
	return BackendName
}

func (k *KubeMQ) Close(_ context.Context) error {
	return k.client.Close()
}

func (k *KubeMQ) Test(_ context.Context) error {
	return types.NotImplementedErr
}
