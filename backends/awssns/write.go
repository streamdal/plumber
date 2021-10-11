package awssns

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/pkg/errors"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber-schemas/build/go/protos/records"

	"github.com/batchcorp/plumber/util"
)

func (a *AWSSNS) Write(_ context.Context, writeOpts *opts.WriteOptions, errorCh chan *records.ErrorRecord, messages ...*records.WriteRecord) error {
	if err := validateWriteOptions(writeOpts); err != nil {
		return errors.Wrap(err, "unable to validate write options")
	}

	topic := writeOpts.Awssns.Args.Topic

	for _, msg := range messages {
		result, err := a.Service.Publish(&sns.PublishInput{
			Message:  aws.String(msg.Input),
			TopicArn: aws.String(topic),
		})
		if err != nil {
			util.WriteError(a.log, errorCh, fmt.Errorf("unable to publish message to topic '%s': %s", topic, err))
			continue
		}

		a.log.Infof("Message '%s' published to topic '%s'", *result.MessageId, topic)
		return nil
	}

	return nil
}

func validateWriteOptions(writeOpts *opts.WriteOptions) error {
	if writeOpts == nil {
		return errors.New("write options cannot be nil")
	}

	if writeOpts.Awssns == nil {
		return errors.New("backend group options cannot be nil")
	}

	if writeOpts.Awssns.Args == nil {
		return errors.New("backend arg options cannot be nil")
	}

	topic := writeOpts.Awssns.Args.Topic

	if topic == "" {
		return ErrMissingTopicARN
	}

	if arn.IsARN(topic) == false {
		return fmt.Errorf("'%s' is not a valid ARN", topic)
	}
	return nil
}
