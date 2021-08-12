package awssns

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	types2 "github.com/batchcorp/plumber/types"
	"github.com/batchcorp/plumber/util"
	"github.com/pkg/errors"
)

var (
	errMissingTopicARN = errors.New("--topic cannot be empty")
)

func (a *AWSSNS) Write(ctx context.Context, errorCh chan *types2.ErrorMessage, messages ...*types2.WriteMessage) error {
	for _, msg := range messages {
		if err := a.write(msg.Value); err != nil {
			util.WriteError(a.log, errorCh, errors.Wrap(err, "unable to write message"))
		}
	}

	return nil
}

func (a *AWSSNS) write(value []byte) error {
	result, err := a.service.Publish(&sns.PublishInput{
		Message:  aws.String(string(value)),
		TopicArn: aws.String(a.Options.AWSSNS.TopicArn),
	})
	if err != nil {
		return errors.Wrap(err, "could not publish message to SNS")
	}

	a.log.Infof("Message '%s' published to topic '%s'", *result.MessageId, a.Options.AWSSNS.TopicArn)
	return nil
}
