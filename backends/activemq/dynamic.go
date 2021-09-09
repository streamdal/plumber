package activemq

import (
	"context"

	"github.com/batchcorp/plumber/dproxy"
	"github.com/batchcorp/plumber/options"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and
// receives a stream of outbound replay messages which are then written to the
// message bus.
func (a *ActiveMq) Dynamic(ctx context.Context) error {
	llog := logrus.WithField("pkg", "activemq/dynamic")

	conn, err := newConn(ctx, a.ConnectionConfig)
	if err != nil {
		return errors.Wrap(err, "unable to create new connection")
	}

	// Start up dynamic connection
	grpc, err := dproxy.New(a.ConnectionConfig, "ActiveMQ")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	destination := getDestination(a.ConnectionConfig)

	// Continually loop looking for messages on the channel.
MAIN:
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			if err := conn.Send(destination, "", outbound.Blob, nil); err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				break
			}

			llog.Debugf("Replayed message to ActiveMQ '%s' for replay '%s'", destination, outbound.ReplayId)
		case <-ctx.Done():
			llog.Debug("asked to exit via context")
			break MAIN
		}
	}

	return nil
}

func getDestination(opts *options.Options) string {
	if opts.ActiveMq.Topic != "" {
		return "/topic/" + opts.ActiveMq.Topic
	}
	return opts.ActiveMq.Queue
}
