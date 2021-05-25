package azure

import (
	"context"

	"github.com/batchcorp/plumber/writer"

	servicebus "github.com/Azure/azure-service-bus-go"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/dproxy"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func Dynamic(opts *cli.Options) error {
	if err := writer.ValidateWriteOptions(opts, validateWriteOptions); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	var queue *servicebus.Queue
	var topic *servicebus.Topic

	ctx := context.Background()
	llog := logrus.WithField("pkg", "azure/dynamic")

	// Start up writer
	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create Azure client")
	}

	if opts.Azure.Queue != "" {
		queue, err = client.NewQueue(opts.Azure.Queue)
		if err != nil {
			return errors.Wrap(err, "unable to create new azure service bus queue client")
		}

		defer queue.Close(ctx)
	} else {
		topic, err = client.NewTopic(opts.Azure.Topic)
		if err != nil {
			return errors.Wrap(err, "unable to create new azure service bus topic client")
		}

		defer topic.Close(ctx)
	}

	// Start up dynamic connection
	grpc, err := dproxy.New(opts, "Azure Service Bus")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			msg := servicebus.NewMessage(outbound.Blob)

			if queue != nil {
				// Publishing to queue
				if err := queue.Send(ctx, msg); err != nil {
					llog.Errorf("Unable to replay message: %s", err)
					break
				}
			} else {
				// Publishing to topic
				if err := topic.Send(ctx, msg); err != nil {
					llog.Errorf("Unable to replay message: %s", err)
					break
				}
			}

			llog.Debugf("Replayed message to Azure Servuce Bus for replay '%s'", outbound.ReplayId)
		}
	}
}
