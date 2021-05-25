package awssns

import (
	"github.com/aws/aws-sdk-go/service/sns"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/dproxy"
	"github.com/batchcorp/plumber/writer"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func Dynamic(opts *cli.Options) error {
	if err := writer.ValidateWriteOptions(opts, validateWriteOptions); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	llog := logrus.WithField("pkg", "gcppubsub/dynamic")

	// Start up writer
	svc, err := NewService(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create SNS service")
	}

	// Start up dynamic connection
	grpc, err := dproxy.New(opts, "AWS SNS")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:
			_, err := svc.Publish(&sns.PublishInput{
				Message:  aws.String(string(outbound.Blob)),
				TopicArn: aws.String(opts.AWSSNS.TopicArn),
			})
			if err != nil {
				llog.Errorf("Unable to replay message: %s", err)
				break
			}

			llog.Debugf("Replayed message to AWSSNS topic '%s' for replay '%s'", opts.AWSSNS.TopicArn, outbound.ReplayId)
		}
	}
}
