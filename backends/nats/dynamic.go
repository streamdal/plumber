package nats

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/dynamic"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func (n *Nats) Dynamic(ctx context.Context) error {
	llog := logrus.WithField("pkg", "nats/dynamic")

	// Start up dynamic connection
	grpc, err := dynamic.New(n.Options, "Nats")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	// Continually loop looking for messages on the channel.
MAIN:
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			if err := n.client.Publish(n.Options.Nats.Subject, outbound.Blob); err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				break
			}

			llog.Debugf("Replayed message to Nats topic '%s' for replay '%s'", n.Options.Nats.Subject, outbound.ReplayId)
		case <-ctx.Done():
			llog.Debug("context cancelled")
			break MAIN
		}
	}

	llog.Debug("exiting")

	return nil
}
