package awssns

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/dynamic"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func (a *AWSSNS) Dynamic(ctx context.Context) error {
	llog := logrus.WithField("pkg", "awssns/dynamic")

	// Start up dynamic connection
	grpc, err := dynamic.New(a.Options, "AWS SNS")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	// TODO: grpc server should accept ctx
	go grpc.Start()

	// Continually loop looking for messages on the channel.
MAIN:
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			_, err := a.service.Publish(&sns.PublishInput{
				Message:  aws.String(string(outbound.Blob)),
				TopicArn: aws.String(a.Options.AWSSNS.TopicArn),
			})
			if err != nil {
				llog.Errorf("Unable to replay message: %s", err)
			}

			llog.Debugf("Replayed message to AWSSNS topic '%s' for replay '%s'", a.Options.AWSSNS.TopicArn, outbound.ReplayId)
		case <-ctx.Done():
			llog.Warning("asked to exit via context")
			break MAIN
		}
	}

	llog.Debug("exiting")

	return nil
}
