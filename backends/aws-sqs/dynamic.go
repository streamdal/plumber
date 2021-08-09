package awssqs

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/dproxy"
	"github.com/batchcorp/plumber/options"
	"github.com/batchcorp/plumber/writer"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func Dynamic(opts *options.Options) error {
	if err := writer.ValidateWriteOptions(opts, validateWriteOptions); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	log := logrus.WithField("pkg", "gcppubsub/dynamic")

	// Start up writer
	svc, queueURL, err := NewService(opts)
	if err != nil {
		return errors.Wrap(err, "unable to create SQS service")
	}

	// Start up dynamic connection
	grpc, err := dproxy.New(opts, "AWS SQS")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:

			input := &sqs.SendMessageInput{
				DelaySeconds:      aws.Int64(opts.AWSSQS.WriteDelaySeconds),
				MessageBody:       aws.String(string(outbound.Blob)),
				QueueUrl:          aws.String(queueURL),
				MessageAttributes: make(map[string]*sqs.MessageAttributeValue, 0),
			}

			// This attribute is required for FIFO queues but cannot be present on requests to non-FIFO queues
			if strings.HasSuffix(opts.AWSSQS.QueueName, ".fifo") {
				input.MessageGroupId = aws.String(opts.AWSSQS.WriteMessageGroupID)
			}

			for k, v := range opts.AWSSQS.WriteAttributes {
				input.MessageAttributes[k] = &sqs.MessageAttributeValue{
					StringValue: aws.String(v),
				}
			}

			if len(input.MessageAttributes) == 0 {
				input.MessageAttributes = nil
			}

			if _, err := svc.SendMessage(input); err != nil {
				log.Errorf("unable to replay message: %s", err)
				break
			}

			log.Debugf("Replayed message to AQSSQS queue '%s' for replay '%s'", opts.AWSSQS.QueueName, outbound.ReplayId)
		}
	}
}
