package awssqs

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber/dynamic"
)

// Dynamic starts up a new GRPC client connected to the dProxy service and receives a stream of outbound replay messages
// which are then written to the message bus.
func (a *AWSSQS) Dynamic(ctx context.Context) error {
	// Do not use writer.validateWriteOptions() in dynamic mode
	if err := validateWriteOptions(a.Options); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	log := logrus.WithField("pkg", "gcppubsub/dynamic")

	// Start up dynamic connection
	grpc, err := dynamic.New(a.Options, "AWS SQS")
	if err != nil {
		return errors.Wrap(err, "could not establish connection to Batch")
	}

	go grpc.Start()

	// Continually loop looking for messages on the channel.
	for {
		select {
		case outbound := <-grpc.OutboundMessageCh:

			input := &sqs.SendMessageInput{
				DelaySeconds:      aws.Int64(a.Options.AWSSQS.WriteDelaySeconds),
				MessageBody:       aws.String(string(outbound.Blob)),
				QueueUrl:          aws.String(a.queueURL),
				MessageAttributes: make(map[string]*sqs.MessageAttributeValue, 0),
			}

			// This attribute is required for FIFO queues but cannot be present on requests to non-FIFO queues
			if strings.HasSuffix(a.Options.AWSSQS.QueueName, ".fifo") {
				input.MessageGroupId = aws.String(a.Options.AWSSQS.WriteMessageGroupID)
			}

			for k, v := range a.Options.AWSSQS.WriteAttributes {
				input.MessageAttributes[k] = &sqs.MessageAttributeValue{
					StringValue: aws.String(v),
				}
			}

			if len(input.MessageAttributes) == 0 {
				input.MessageAttributes = nil
			}

			if _, err := a.service.SendMessage(input); err != nil {
				log.Errorf("unable to replay message: %s", err)
				break
			}

			log.Debugf("Replayed message to AQSSQS queue '%s' for replay '%s'", a.Options.AWSSQS.QueueName, outbound.ReplayId)
		}
	}
}
