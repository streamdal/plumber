package azure

import (
	"context"

	serviceBus "github.com/Azure/azure-service-bus-go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/dproxy"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func (s *ServiceBus) Dynamic(ctx context.Context) error {
	// Do not use writer.validateWriteOptions() in dynamic mode
	if err := validateWriteOptions(s.Options); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	llog := logrus.WithField("pkg", "azure/dynamic")

	// Start up dynamic connection
	grpc, err := dproxy.New(s.Options, "Azure Service Bus")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	// Continually loop looking for messages on the channel.
MAIN:
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			msg := serviceBus.NewMessage(outbound.Blob)

			if s.queue != nil {
				// Publishing to queue
				if err := s.queue.Send(ctx, msg); err != nil {
					llog.Errorf("Unable to replay message: %s", err)
					break
				}
			} else {
				// Publishing to topic
				if err := s.topic.Send(ctx, msg); err != nil {
					llog.Errorf("Unable to replay message: %s", err)
					break
				}
			}

			llog.Debugf("Replayed message to Azure Service Bus for replay '%s'", outbound.ReplayId)
		case <-ctx.Done():
			llog.Debug("context has been cancelled")
			break MAIN
		}
	}

	llog.Debug("exiting")

	return nil
}
