package kubemq_queue

import (
	"context"

	"github.com/kubemq-io/kubemq-go/queues_stream"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/dproxy"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func Dynamic(opts *cli.Options) error {
	llog := logrus.WithField("pkg", "kubemq-queue/dynamic")

	// Start up client
	client, err := NewClient(opts)
	if err != nil {
		return errors.Wrap(err, "unable to initialize kubemq-queue publisher")
	}

	defer func() {
		_ = client.Close()
	}()

	// Start up dynamic connection
	grpc, err := dproxy.New(opts, "KubeMQ-Queue")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			message := queues_stream.NewQueueMessage().
				SetChannel(opts.KubeMQQueue.Queue).SetBody(outbound.Blob)

			results, err := client.Send(context.Background(), message)
			if err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				break
			}
			if len(results.Results) > 0 {
				result := results.Results[0]
				if result.IsError {
					llog.Errorf("Unable to replay message: %s", result.Error)
					break
				} else {
					llog.Debugf("Replayed message to kubemq-queue queue '%s' for replay '%s'", opts.KubeMQQueue.Queue, outbound.ReplayId)
				}
			}
		}
	}
}
